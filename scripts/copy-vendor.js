import { cpSync, mkdirSync } from "fs";
import { dirname, resolve } from "path";
import { fileURLToPath } from "url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const dest = resolve(__dirname, "../web/public/vendor");

mkdirSync(dest, { recursive: true });

cpSync(
  resolve(
    __dirname,
    "../node_modules/vis-network/standalone/esm/vis-network.min.js",
  ),
  resolve(dest, "vis-network.min.js"),
);

console.log("Vendor files copied to web/public/vendor/");
