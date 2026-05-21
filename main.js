const { app, BrowserWindow, desktopCapturer } = require('electron');
const path = require('path');
const { createSignalingServer } = require('./server');

// Expose real LAN IPs in WebRTC ICE candidates instead of mDNS .local hostnames.
// Without this, two machines on the same Wi-Fi can't connect directly.
app.commandLine.appendSwitch('disable-features', 'WebRtcHideLocalIpsWithMdns');

let signalingServer;

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

  // Intercept getDisplayMedia and auto-select the primary screen
  win.webContents.session.setDisplayMediaRequestHandler((_req, callback) => {
    desktopCapturer.getSources({ types: ['screen'] }).then(sources => {
      callback({ video: sources[0] });
    });
  });

  // Start server first to get the dynamic port, then pass it to the renderer
  signalingServer = await createSignalingServer(win);

  win.loadFile(path.join(__dirname, 'src', 'index.html'), {
    query: { wsPort: String(signalingServer.port) },
  });
});

app.on('window-all-closed', () => {
  signalingServer?.close();
  if (process.platform !== 'darwin') app.quit();
});
