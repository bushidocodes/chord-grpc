const crypto = require("crypto");
const { parentPort, workerData } = require("worker_threads");
parentPort.postMessage(
  crypto.createHash("sha1").update(workerData).digest("hex"),
);
