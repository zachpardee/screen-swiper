const { app, BrowserWindow, desktopCapturer, ipcMain } = require('electron');
const path = require('path');
const os = require('os');
const { createSignalingServer } = require('./server');

// Expose real LAN IPs in WebRTC ICE candidates instead of mDNS .local hostnames.
// Without this, two machines on the same Wi-Fi can't connect directly.
app.commandLine.appendSwitch('disable-features', 'WebRtcHideLocalIpsWithMdns');

let signalingServer;
let pendingSourceId = null;

ipcMain.handle('get-screen-sources', async () => {
  const sources = await desktopCapturer.getSources({ types: ['screen'] });
  return sources.map(s => ({ id: s.id, name: s.name }));
});

ipcMain.handle('set-next-source', (_, id) => { pendingSourceId = id; });

app.whenReady().then(async () => {
  const win = new BrowserWindow({
    width: 1280,
    height: 800,
    minWidth: 800,
    minHeight: 600,
    backgroundColor: '#0d0d12',
    titleBarStyle: 'hiddenInset',
    webPreferences: {
      preload: path.join(__dirname, 'preload.js'),
      contextIsolation: true,
      nodeIntegration: false,
    },
  });

  // Intercept getDisplayMedia — use pendingSourceId to select a specific monitor
  win.webContents.session.setDisplayMediaRequestHandler((_req, callback) => {
    desktopCapturer.getSources({ types: ['screen'] }).then(sources => {
      const source = pendingSourceId
        ? (sources.find(s => s.id === pendingSourceId) || sources[0])
        : sources[0];
      pendingSourceId = null;
      callback({ video: source });
    });
  });

  // Start server first to get the dynamic port, then pass it to the renderer
  signalingServer = await createSignalingServer(win);

  win.loadFile(path.join(__dirname, 'src', 'index.html'), {
    query: { wsPort: String(signalingServer.port), hostname: os.hostname() },
  });
});

app.on('window-all-closed', () => {
  signalingServer?.close();
  if (process.platform !== 'darwin') app.quit();
});
