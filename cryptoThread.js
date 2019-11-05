const CryptoJS = require("crypto-js");
const { parentPort, workerData } = require("worker_threads");
parentPort.postMessage(CryptoJS.SHA1(workerData).toString());
