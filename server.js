const WebSocket = require('ws');
const dgram = require('dgram');
const os = require('os');
const crypto = require('crypto');

const WS_PORT = 3456;
const UDP_PORT = 3457;
const BEACON_MS = 2000;

const MY_ID = crypto.randomUUID();
const MY_NAME = os.hostname();

function getLocalIPs() {
  const ips = [];
  for (const iface of Object.values(os.networkInterfaces())) {
    for (const a of iface) {
      if (a.family === 'IPv4' && !a.internal) ips.push(a.address);
    }
  }
  return ips;
}

function getBroadcastAddresses() {
  const addrs = new Set(['255.255.255.255']);
  for (const iface of Object.values(os.networkInterfaces())) {
    for (const a of iface) {
      if (a.family === 'IPv4' && !a.internal && a.netmask) {
        const ip = a.address.split('.').map(Number);
        const mask = a.netmask.split('.').map(Number);
        const bc = ip.map((b, i) => (b | (~mask[i] & 255))).join('.');
        addrs.add(bc);
      }
    }
  }
  return [...addrs];
}

function startWss(preferredPort) {
  return new Promise((resolve, reject) => {
    const wss = new WebSocket.Server({ port: preferredPort });
    wss.on('listening', () => resolve(wss));
    wss.on('error', err => {
      if (err.code === 'EADDRINUSE') {
        const fallback = new WebSocket.Server({ port: 0 });
        fallback.on('listening', () => resolve(fallback));
        fallback.on('error', reject);
      } else {
        reject(err);
      }
    });
  });
}

async function createSignalingServer(mainWindow) {
  const peers = new Map();   // id -> { ws, name, ip }
  const seenIDs = new Set([MY_ID]);
  let rendererWs = null;
  const rendererQueue = []; // messages buffered before renderer connects

  function toRenderer(msg) {
    if (rendererWs?.readyState === WebSocket.OPEN) {
      rendererWs.send(JSON.stringify(msg));
    } else {
      rendererQueue.push(msg);
    }
  }

  const wss = await startWss(WS_PORT);
  const wsPort = wss.address().port;

  wss.on('connection', (ws, req) => {
    const ip = req.socket.remoteAddress.replace(/^::ffff:/, '');
    const isLocal = ip === '127.0.0.1' || ip === '::1';

    if (isLocal) {
      rendererWs = ws;
      ws.send(JSON.stringify({ type: 'init', id: MY_ID, name: MY_NAME }));
      ws.send(JSON.stringify({
        type: 'peer-list',
        peers: [...peers.entries()].map(([id, p]) => ({ id, name: p.name, ip: p.ip })),
      }));
      // Flush any messages that arrived before the renderer connected
      for (const msg of rendererQueue.splice(0)) {
        ws.send(JSON.stringify(msg));
      }

      ws.on('message', raw => {
        try {
          const msg = JSON.parse(raw);
          if (msg.to) {
            const peer = peers.get(msg.to);
            if (peer?.ws.readyState === WebSocket.OPEN) {
              peer.ws.send(JSON.stringify({ ...msg, from: MY_ID }));
            }
          }
        } catch {}
      });

      ws.on('close', () => { rendererWs = null; });
      return;
    }

    // Incoming remote peer
    let peerId;
    ws.on('message', raw => {
      try {
        const msg = JSON.parse(raw);
        if (msg.type === 'hello') {
          if (seenIDs.has(msg.id)) { ws.close(); return; }
          peerId = msg.id;
          seenIDs.add(peerId);
          peers.set(peerId, { ws, name: msg.name, ip });
          ws.send(JSON.stringify({ type: 'hello-ack', id: MY_ID, name: MY_NAME }));
          toRenderer({ type: 'peer-connected', id: peerId, name: msg.name, ip });
        } else if (['offer', 'answer', 'ice'].includes(msg.type)) {
          toRenderer({ ...msg, from: peerId });
        }
      } catch {}
    });

    ws.on('close', () => {
      if (peerId) {
        peers.delete(peerId);
        seenIDs.delete(peerId);
        toRenderer({ type: 'peer-disconnected', id: peerId });
      }
    });
  });

  function connectToPeer(ip, port, remoteId, remoteName) {
    if (seenIDs.has(remoteId)) return;
    seenIDs.add(remoteId);

    const ws = new WebSocket(`ws://${ip}:${port}`);

    ws.on('open', () => {
      ws.send(JSON.stringify({ type: 'hello', id: MY_ID, name: MY_NAME }));
    });

    ws.on('message', raw => {
      try {
        const msg = JSON.parse(raw);
        if (msg.type === 'hello-ack') {
          peers.set(remoteId, { ws, name: msg.name, ip });
          toRenderer({ type: 'peer-connected', id: remoteId, name: msg.name, ip });
        } else if (['offer', 'answer', 'ice'].includes(msg.type)) {
          toRenderer({ ...msg, from: remoteId });
        }
      } catch {}
    });

    ws.on('close', () => {
      peers.delete(remoteId);
      seenIDs.delete(remoteId);
      toRenderer({ type: 'peer-disconnected', id: remoteId });
    });

    ws.on('error', () => {
      seenIDs.delete(remoteId);
    });
  }

  // UDP peer discovery — lower UUID always initiates to avoid duplicate connections.
  // Filter by device ID only (not IP) so two instances on the same machine can discover each other.
  const udp = dgram.createSocket({ type: 'udp4', reuseAddr: true });
  udp.bind(UDP_PORT, () => udp.setBroadcast(true));

  udp.on('message', (data, rinfo) => {
    try {
      const b = JSON.parse(data.toString());
      if (b.type === 'announce' && b.id && b.id !== MY_ID && MY_ID < b.id) {
        connectToPeer(rinfo.address, b.port, b.id, b.name || '');
      }
    } catch {}
  });

  udp.on('error', err => console.error('[UDP]', err.message));

  const beaconMsg = Buffer.from(
    JSON.stringify({ type: 'announce', id: MY_ID, name: MY_NAME, port: wsPort })
  );
  const broadcasts = getBroadcastAddresses();

  function sendBeacon() {
    for (const bc of broadcasts) {
      try { udp.send(beaconMsg, UDP_PORT, bc); } catch {}
    }
  }

  const timer = setInterval(sendBeacon, BEACON_MS);
  sendBeacon();

  return {
    port: wsPort,
    close() {
      clearInterval(timer);
      wss.close();
      try { udp.close(); } catch {}
    },
  };
}

module.exports = { createSignalingServer };
