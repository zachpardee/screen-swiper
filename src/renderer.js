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
    sendOffer(peerId);
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

  // Renegotiate when new tracks are added (e.g. secondary monitor captured after connection)
  pc.onnegotiationneeded = async () => {
    if (pc.signalingState !== 'stable') return;
    try {
      const offer = await pc.createOffer();
      if (pc.signalingState !== 'stable') return;
      await pc.setLocalDescription(offer);
      ws.send(JSON.stringify({ type: 'offer', to: peerId, sdp: pc.localDescription }));
    } catch {}
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
  const pc = getOrCreatePC(peerId);
  const offer = await pc.createOffer();
  await pc.setLocalDescription(offer);
  ws.send(JSON.stringify({ type: 'offer', to: peerId, sdp: pc.localDescription }));
}

async function onOffer(from, sdp) {
  // Wait for local stream so our tracks are included in the answer
  if (!streamReady) await streamReadyPromise;
  const pc = getOrCreatePC(from);
  // If we have a pending local offer (collision), the lower UUID wins and ignores this offer
  if (pc.signalingState === 'have-local-offer' && myId < from) return;
  await pc.setRemoteDescription(new RTCSessionDescription(sdp));
  const answer = await pc.createAnswer();
  await pc.setLocalDescription(answer);
  ws.send(JSON.stringify({ type: 'answer', to: from, sdp: pc.localDescription }));
}

async function onAnswer(from, sdp) {
  const pc = connections.get(from);
  if (pc) await pc.setRemoteDescription(new RTCSessionDescription(sdp));
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
        for (const peerId of pendingOffers.splice(0)) sendOffer(peerId);
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

let currentSlide = 0;

function setStatus(state, label) {
  document.getElementById('status-dot').className = `status-dot ${state}`;
  document.getElementById('status-label').textContent = label;
}

function attachStream(peerId, stream) {
  const tile = document.querySelector(`[data-peer="${peerId}"]`);
  if (!tile) return;
  const video = tile.querySelector('video');
  video.srcObject = stream;
  tile.classList.remove('connecting');
}

function showLocalTile(tileId, stream, label) {
  const swiper = document.getElementById('screens-swiper');
  document.getElementById('swiper-wrap').style.display = 'flex';
  document.getElementById('empty-state').style.display = 'none';

  let tile = swiper.querySelector(`[data-peer="${tileId}"]`);
  if (!tile) {
    tile = buildTile(tileId, { name: label });
    // Insert after last local tile so local screens always stay at front
    const locals = [...swiper.querySelectorAll('[data-peer^="__local"]')];
    const last = locals[locals.length - 1];
    last ? last.insertAdjacentElement('afterend', tile) : swiper.insertBefore(tile, swiper.firstChild);
  }

  tile.querySelector('video').srcObject = stream;
  tile.classList.remove('connecting');
  renderNavDots();
  updateArrows();
}

function renderNavDots() {
  const swiper = document.getElementById('screens-swiper');
  const nav = document.getElementById('swiper-nav');
  const count = swiper.querySelectorAll('[data-peer]').length;
  nav.style.display = count > 1 ? 'flex' : 'none';
  nav.innerHTML = '';
  for (let i = 0; i < count; i++) {
    const dot = document.createElement('button');
    dot.className = `nav-dot${i === currentSlide ? ' active' : ''}`;
    dot.setAttribute('aria-label', `Screen ${i + 1}`);
    dot.addEventListener('click', () => goTo(i));
    nav.appendChild(dot);
  }
}

function renderScreens() {
  const swiper = document.getElementById('screens-swiper');
  const wrap = document.getElementById('swiper-wrap');
  const nav = document.getElementById('swiper-nav');
  const empty = document.getElementById('empty-state');
  const peerIds = [...peers.keys()];

  if (peerIds.length === 0 && !localStream) {
    wrap.style.display = 'none';
    nav.style.display = 'none';
    empty.style.display = 'flex';
    return;
  }

  empty.style.display = 'none';
  wrap.style.display = 'flex';

  // Remove tiles for gone peers (preserve local tiles and secondary monitor tiles)
  swiper.querySelectorAll('[data-peer]').forEach(el => {
    const id = el.dataset.peer;
    if (id.startsWith('__local')) return;
    const belongsToKnownPeer = peers.has(id) || [...peers.keys()].some(p => id.startsWith(p + '_'));
    if (!belongsToKnownPeer) el.remove();
  });

  // Add tiles for new peers
  const existing = new Set([...swiper.querySelectorAll('[data-peer]')].map(el => el.dataset.peer));
  for (const peerId of peerIds) {
    if (!existing.has(peerId)) {
      const peer = peers.get(peerId);
      const tile = buildTile(peerId, peer);
      swiper.appendChild(tile);
      if (peer.stream) attachStream(peerId, peer.stream);
    }
  }

  renderNavDots();
  updateArrows();
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
  fsBtn.title = 'Fullscreen (double-click)';
  fsBtn.innerHTML = '<svg width="16" height="16" viewBox="0 0 16 16" fill="none"><path d="M2 6V2h4M10 2h4v4M14 10v4h-4M6 14H2v-4" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/></svg>';
  fsBtn.addEventListener('click', e => { e.stopPropagation(); toggleFullscreen(video); });

  const spinner = document.createElement('div');
  spinner.className = 'connecting-overlay';
  spinner.innerHTML = '<div class="spinner"></div><span>Connecting…</span>';

  overlay.appendChild(label);
  overlay.appendChild(fsBtn);
  tile.appendChild(video);
  tile.appendChild(overlay);
  tile.appendChild(spinner);

  tile.addEventListener('dblclick', () => toggleFullscreen(video));

  return tile;
}

function toggleFullscreen(el) {
  if (document.fullscreenElement) document.exitFullscreen();
  else el.requestFullscreen().catch(() => {});
}

function goTo(index) {
  const swiper = document.getElementById('screens-swiper');
  const cards = swiper.querySelectorAll('.screen-card');
  if (!cards[index]) return;
  currentSlide = index;
  swiper.scrollTo({ left: cards[index].offsetLeft, behavior: 'smooth' });
  document.querySelectorAll('.nav-dot').forEach((d, i) => d.classList.toggle('active', i === index));
  updateArrows();
}

function updateArrows() {
  const swiper = document.getElementById('screens-swiper');
  const count = swiper.querySelectorAll('.screen-card').length;
  document.getElementById('arrow-left').style.opacity = currentSlide > 0 ? '1' : '0';
  document.getElementById('arrow-right').style.opacity = currentSlide < count - 1 ? '1' : '0';
}

// ── Init ───────────────────────────────────────────────────────────────────

document.addEventListener('DOMContentLoaded', () => {
  document.getElementById('device-name').textContent = MY_HOSTNAME;

  document.getElementById('arrow-left').addEventListener('click', () => goTo(currentSlide - 1));
  document.getElementById('arrow-right').addEventListener('click', () => goTo(currentSlide + 1));

  document.addEventListener('keydown', e => {
    if (e.key === 'ArrowLeft') goTo(currentSlide - 1);
    if (e.key === 'ArrowRight') goTo(currentSlide + 1);
  });

  document.getElementById('screens-swiper').addEventListener('scroll', function () {
    const cards = this.querySelectorAll('.screen-card');
    if (!cards.length) return;
    currentSlide = Math.round(this.scrollLeft / this.offsetWidth);
    document.querySelectorAll('.nav-dot').forEach((d, i) => d.classList.toggle('active', i === currentSlide));
    updateArrows();
  });

  connect();
});
