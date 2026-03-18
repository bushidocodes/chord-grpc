import crypto from "crypto";
import { parentPort, workerData } from "worker_threads";
parentPort.postMessage(
  crypto.createHash("sha1").update(workerData).digest("hex"),
);
