const { contextBridge } = require('electron');
const os = require('os');

contextBridge.exposeInMainWorld('electronAPI', {
  hostname: os.hostname(),
});
