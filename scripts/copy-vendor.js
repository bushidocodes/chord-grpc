import { cpSync, mkdirSync } from "fs";
import { resolve, dirname } from "path";
import { fileURLToPath } from "url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const dest = resolve(__dirname, "../web/public/vendor");

mkdirSync(dest, { recursive: true });

cpSync(
  resolve(
    __dirname,
    "../node_modules/vis-network/standalone/umd/vis-network.min.js",
  ),
  resolve(dest, "vis-network.min.js"),
);

cpSync(
  resolve(__dirname, "../node_modules/lodash/lodash.min.js"),
  resolve(dest, "lodash.min.js"),
);

console.log("Vendor files copied to web/public/vendor/");
