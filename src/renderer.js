const ICE_CONFIG = {
  iceServers: [
    { urls: 'stun:stun.l.google.com:19302' },
    { urls: 'stun:stun1.l.google.com:19302' },
    { urls: 'stun:stun.cloudflare.com:3478' },
  ],
};

let ws;
let myId = null;
let localStream = null;
let streamReady = false;
let streamReadyResolve = null;
const streamReadyPromise = new Promise(r => { streamReadyResolve = r; });

const peers = new Map();          // peerId -> { name, ip, stream? }
const connections = new Map();    // peerId -> RTCPeerConnection
const pendingOffers = [];         // peerIds waiting for localStream
const localStreamsList = [];      // all captured local streams, in order
const peerStreams = new Map();     // peerId -> Set<stream.id> already tiled

// ── WebSocket ──────────────────────────────────────────────────────────────

const wsPort = new URLSearchParams(location.search).get('wsPort') || '3456';
const MY_HOSTNAME = new URLSearchParams(location.search).get('hostname') || 'You';

function connect() {
  ws = new WebSocket(`ws://localhost:${wsPort}`);

  ws.onopen = async () => {
    setStatus('connected', 'On network');
    if (!streamReady) await startLocalStream();
  };

  ws.onclose = () => {
    setStatus('disconnected', 'Reconnecting…');
    setTimeout(connect, 1500);
  };

  ws.onerror = () => {};

  ws.onmessage = async ({ data }) => {
    try { await handle(JSON.parse(data)); } catch (e) { console.error(e); }
  };
}

async function handle(msg) {
  switch (msg.type) {
    case 'init':
      myId = msg.id;
      document.getElementById('device-name').textContent = msg.name;
      break;

    case 'peer-list':
      for (const p of msg.peers) {
        if (!peers.has(p.id)) {
          peers.set(p.id, { name: p.name, ip: p.ip });
          maybeOffer(p.id);
        }
      }
      renderScreens();
      break;

    case 'peer-connected':
      if (!peers.has(msg.id)) {
        peers.set(msg.id, { name: msg.name, ip: msg.ip });
        maybeOffer(msg.id);
        renderScreens();
      }
      break;

    case 'peer-disconnected':
      peers.delete(msg.id);
      dropConnection(msg.id);
      renderScreens();
      break;

    case 'offer':
      await onOffer(msg.from, msg.sdp);
      break;

    case 'answer':
      await onAnswer(msg.from, msg.sdp);
      break;

    case 'ice':
      await onIce(msg.from, msg.candidate);
      break;
  }
}

// ── WebRTC ─────────────────────────────────────────────────────────────────

function maybeOffer(peerId) {
  // Lower UUID is always the offerer to prevent both sides sending an offer
  if (!myId || myId >= peerId) return;
  if (streamReady) {
    getOrCreatePC(peerId); // adding tracks triggers onnegotiationneeded, which sends the offer
  } else {
    pendingOffers.push(peerId);
  }
}

function getOrCreatePC(peerId) {
  if (connections.has(peerId)) return connections.get(peerId);

  const pc = new RTCPeerConnection(ICE_CONFIG);
  connections.set(peerId, pc);

  // Add all captured local streams upfront
  for (const stream of localStreamsList) {
    stream.getTracks().forEach(t => pc.addTrack(t, stream));
  }

  pc.onicecandidate = ({ candidate }) => {
    if (candidate && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({ type: 'ice', to: peerId, candidate }));
    }
  };

  // Either side may offer (perfect negotiation); collisions resolved in onOffer
  pc.onnegotiationneeded = async () => {
    if (pc.signalingState !== 'stable') return;
    try {
      const offer = await pc.createOffer();
      if (pc.signalingState !== 'stable') return;
      await pc.setLocalDescription(offer);
      ws.send(JSON.stringify({ type: 'offer', to: peerId, sdp: pc.localDescription }));
    } catch (e) { console.error('onnegotiationneeded:', e); }
  };

  pc.ontrack = ({ streams }) => {
    const stream = streams[0];
    if (!stream) return;

    let seen = peerStreams.get(peerId);
    if (!seen) { seen = new Set(); peerStreams.set(peerId, seen); }
    if (seen.has(stream.id)) return; // already created a tile for this stream
    seen.add(stream.id);

    const idx = seen.size - 1;
    const tileId = idx === 0 ? peerId : `${peerId}_${idx}`;
    const peer = peers.get(peerId) || {};
    peers.set(peerId, { ...peer, stream });

    if (idx > 0) {
      const tile = buildTile(tileId, { name: peer.name || peerId.slice(0, 8) });
      document.getElementById('screens-swiper').appendChild(tile);
      renderNavDots();
      updateArrows();
    }

    attachStream(tileId, stream);
  };

  pc.onconnectionstatechange = () => {
    if (['failed', 'closed'].includes(pc.connectionState)) dropConnection(peerId);
  };

  return pc;
}

async function sendOffer(peerId) {
  const pc = connections.get(peerId);
  if (!pc || pc.signalingState !== 'stable') return;
  try {
    const offer = await pc.createOffer();
    if (pc.signalingState !== 'stable') return;
    await pc.setLocalDescription(offer);
    ws.send(JSON.stringify({ type: 'offer', to: peerId, sdp: pc.localDescription }));
  } catch (e) { console.error('sendOffer:', e); }
}

async function onOffer(from, sdp) {
  // Wait for local stream so our tracks are included in the answer
  if (!streamReady) await streamReadyPromise;
  const pc = getOrCreatePC(from);

  const collision = pc.signalingState !== 'stable';
  const impolite = myId < from; // lower UUID never yields

  if (collision && impolite) return; // impolite peer ignores colliding remote offer

  if (collision) {
    // Polite peer (higher UUID) rolls back its pending offer and accepts the remote one
    await pc.setLocalDescription({ type: 'rollback' });
  }

  await pc.setRemoteDescription(new RTCSessionDescription(sdp));
  const answer = await pc.createAnswer();
  await pc.setLocalDescription(answer);
  ws.send(JSON.stringify({ type: 'answer', to: from, sdp: pc.localDescription }));
}

async function onAnswer(from, sdp) {
  const pc = connections.get(from);
  if (!pc) return;
  await pc.setRemoteDescription(new RTCSessionDescription(sdp));
  // If tracks were added while negotiating (e.g. secondary monitor), renegotiate now
  if (myId < from && pc.getTransceivers().some(t => t.sender.track && !t.mid)) {
    await sendOffer(from);
  }
}

async function onIce(from, candidate) {
  const pc = connections.get(from);
  if (pc && candidate) {
    try { await pc.addIceCandidate(new RTCIceCandidate(candidate)); } catch {}
  }
}

function dropConnection(peerId) {
  const pc = connections.get(peerId);
  if (pc) { pc.close(); connections.delete(peerId); }
  peerStreams.delete(peerId);
  // Remove primary tile and any secondary monitor tiles (uuid_1, uuid_2, …)
  document.querySelectorAll('[data-peer]').forEach(el => {
    const id = el.dataset.peer;
    if (id === peerId || id.startsWith(peerId + '_')) el.remove();
  });
  renderNavDots();
  updateArrows();
}

// ── Screen capture ─────────────────────────────────────────────────────────

async function startLocalStream() {
  try {
    const sources = window.electronAPI?.getScreenSources
      ? await window.electronAPI.getScreenSources()
      : [{ id: null, name: null }];

    for (let i = 0; i < sources.length; i++) {
      const source = sources[i];
      const tileId = i === 0 ? '__local__' : `__local_${i}`;
      const label = i === 0
        ? `${MY_HOSTNAME} (you)`
        : `${source.name || `Screen ${i + 1}`} (you)`;

      if (source.id) await window.electronAPI.setNextSource(source.id);

      const stream = await navigator.mediaDevices.getDisplayMedia({
        video: { frameRate: { ideal: 30 } },
        audio: false,
      });

      localStreamsList.push(stream);

      if (i === 0) {
        localStream = stream;
        streamReady = true;
        streamReadyResolve();

        // Inject primary track into any already-open connections
        for (const [, pc] of connections) {
          stream.getTracks().forEach(t => {
            const sender = pc.getSenders().find(s => s.track?.kind === t.kind);
            if (sender) sender.replaceTrack(t);
            else pc.addTrack(t, stream);
          });
        }
        // Create PCs for deferred peers — addTrack inside triggers onnegotiationneeded
        for (const peerId of pendingOffers.splice(0)) getOrCreatePC(peerId);
      } else {
        // Inject secondary monitor tracks into existing connections (triggers renegotiation)
        for (const [, pc] of connections) {
          stream.getTracks().forEach(t => pc.addTrack(t, stream));
        }
      }

      showLocalTile(tileId, stream, label);

      stream.getVideoTracks()[0].onended = () => {
        localStreamsList.splice(localStreamsList.indexOf(stream), 1);
        const tile = document.querySelector(`[data-peer="${tileId}"]`);
        if (tile) tile.remove();
        renderNavDots();
        updateArrows();
        if (i === 0) {
          streamReady = false;
          localStream = null;
          setTimeout(startLocalStream, 800);
        }
      };
    }
  } catch (err) {
    console.error('getDisplayMedia failed:', err);
    setStatus('error', 'Capture failed');
  }
}

// ── UI ─────────────────────────────────────────────────────────────────────

function setStatus(state, label) {
  document.getElementById('status-dot').className = `status-dot ${state}`;
  document.getElementById('status-label').textContent = label;
}

function attachStream(peerId, stream) {
  const tile = document.querySelector(`[data-peer="${peerId}"]`);
  if (!tile) return;
  tile.querySelector('video').srcObject = stream;
  tile.classList.remove('connecting');
}

function showLocalTile(tileId, stream, label) {
  const grid = document.getElementById('screens-grid');
  document.getElementById('empty-state').style.display = 'none';
  grid.style.display = 'grid';

  let tile = grid.querySelector(`[data-peer="${tileId}"]`);
  if (!tile) {
    tile = buildTile(tileId, { name: label });
    // Local tiles always go first
    const locals = [...grid.querySelectorAll('[data-peer^="__local"]')];
    const last = locals[locals.length - 1];
    last ? last.insertAdjacentElement('afterend', tile) : grid.insertBefore(tile, grid.firstChild);
  }

  tile.querySelector('video').srcObject = stream;
  tile.classList.remove('connecting');
  updateGridColumns();
}

function renderScreens() {
  const grid = document.getElementById('screens-grid');
  const empty = document.getElementById('empty-state');
  const peerIds = [...peers.keys()];

  if (peerIds.length === 0 && !localStream) {
    grid.style.display = 'none';
    empty.style.display = 'flex';
    return;
  }

  empty.style.display = 'none';
  grid.style.display = 'grid';

  // Remove tiles for gone peers (preserve local tiles and secondary monitor tiles)
  grid.querySelectorAll('[data-peer]').forEach(el => {
    const id = el.dataset.peer;
    if (id.startsWith('__local')) return;
    const known = peers.has(id) || [...peers.keys()].some(p => id.startsWith(p + '_'));
    if (!known) el.remove();
  });

  // Add tiles for new peers
  const existing = new Set([...grid.querySelectorAll('[data-peer]')].map(el => el.dataset.peer));
  for (const peerId of peerIds) {
    if (!existing.has(peerId)) {
      const peer = peers.get(peerId);
      const tile = buildTile(peerId, peer);
      grid.appendChild(tile);
      if (peer.stream) attachStream(peerId, peer.stream);
    }
  }

  updateGridColumns();
}

function updateGridColumns() {
  const grid = document.getElementById('screens-grid');
  const count = grid.querySelectorAll('.screen-card').length;
  const cols = count <= 1 ? 1 : count <= 4 ? 2 : 3;
  grid.style.gridTemplateColumns = `repeat(${cols}, 1fr)`;
}

function buildTile(peerId, peer) {
  const tile = document.createElement('div');
  tile.className = 'screen-card connecting';
  tile.dataset.peer = peerId;

  const video = document.createElement('video');
  video.autoplay = true;
  video.muted = true;
  video.playsInline = true;

  const overlay = document.createElement('div');
  overlay.className = 'card-overlay';

  const label = document.createElement('div');
  label.className = 'peer-label';
  label.textContent = peer.name || peerId.slice(0, 8);

  const fsBtn = document.createElement('button');
  fsBtn.className = 'fs-btn';
  fsBtn.title = 'Fullscreen';
  fsBtn.innerHTML = '<svg width="14" height="14" viewBox="0 0 16 16" fill="none"><path d="M2 6V2h4M10 2h4v4M14 10v4h-4M6 14H2v-4" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/></svg>';
  fsBtn.addEventListener('click', e => { e.stopPropagation(); toggleFullscreen(video); });

  const spinner = document.createElement('div');
  spinner.className = 'connecting-overlay';
  spinner.innerHTML = '<div class="spinner"></div><span>Connecting…</span>';

  overlay.appendChild(label);
  overlay.appendChild(fsBtn);
  tile.appendChild(video);
  tile.appendChild(overlay);
  tile.appendChild(spinner);

  tile.addEventListener('click', () => openModal(peerId));

  return tile;
}

// ── Modal ──────────────────────────────────────────────────────────────────

function openModal(tileId) {
  const tile = document.querySelector(`[data-peer="${tileId}"]`);
  if (!tile || tile.classList.contains('connecting')) return;
  const stream = tile.querySelector('video').srcObject;
  const label = tile.querySelector('.peer-label')?.textContent || '';

  const overlay = document.getElementById('modal-overlay');
  const video = document.getElementById('modal-video');
  document.getElementById('modal-label').textContent = label;
  video.srcObject = stream;
  overlay.dataset.tileId = tileId;
  overlay.style.display = 'flex';
}

function closeModal() {
  const overlay = document.getElementById('modal-overlay');
  overlay.style.display = 'none';
  document.getElementById('modal-video').srcObject = null;
  delete overlay.dataset.tileId;
}

function toggleFullscreen(el) {
  if (document.fullscreenElement) document.exitFullscreen();
  else el.requestFullscreen().catch(() => {});
}

// ── Init ───────────────────────────────────────────────────────────────────

document.addEventListener('DOMContentLoaded', () => {
  document.getElementById('device-name').textContent = MY_HOSTNAME;

  document.getElementById('modal-close').addEventListener('click', closeModal);
  document.getElementById('modal-fs-btn').addEventListener('click', () => {
    toggleFullscreen(document.getElementById('modal-video'));
  });

  // Click outside the video card closes the modal
  document.getElementById('modal-overlay').addEventListener('click', e => {
    if (e.target === document.getElementById('modal-overlay')) closeModal();
  });

  document.addEventListener('keydown', e => {
    if (e.key === 'Escape') closeModal();
  });

  connect();
});
