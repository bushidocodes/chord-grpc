#!/usr/bin/env node
// Regenerates the typed gRPC stubs in app/generated from protos/*.proto
// (issue #240). The loader options passed to proto-loader-gen-types must
// match LOADER_OPTIONS in app/proto.ts, or the generated types will not
// describe what the runtime actually produces.
//
// proto-loader-gen-types emits extensionless relative imports; this script
// rewrites them to explicit .ts imports, which both tsc (moduleResolution
// nodenext) and consistency with the rest of the codebase require. The
// generated files are type-only, so Node never loads them at runtime.
import { execFileSync } from "node:child_process";
import { readdirSync, readFileSync, writeFileSync } from "node:fs";
import path from "node:path";

const root = path.resolve(import.meta.dirname, "..");
const outDir = path.join(root, "app", "generated");
// Invoke the generator's JS entry point directly: cross-platform (no .cmd
// shim, no shell) and pinned to the @grpc/proto-loader we depend on.
const generatorBin = path.join(
  root,
  "node_modules",
  "@grpc",
  "proto-loader",
  "build",
  "bin",
  "proto-loader-gen-types.js",
);

execFileSync(
  process.execPath,
  [
    generatorBin,
    "--keepCase",
    "--longs=String",
    "--enums=String",
    "--defaults",
    "--oneofs",
    "--grpcLib=@grpc/grpc-js",
    `--outDir=${outDir}`,
    "protos/chord.proto",
    "protos/health.proto",
  ],
  { cwd: root, stdio: "inherit" },
);

function* walk(dir) {
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    const entryPath = path.join(dir, entry.name);
    if (entry.isDirectory()) yield* walk(entryPath);
    else if (entryPath.endsWith(".ts")) yield entryPath;
  }
}

for (const file of walk(outDir)) {
  const source = readFileSync(file, "utf8");
  const fixed = source.replace(/from '(\.[^']*)'/g, "from '$1.ts'");
  if (fixed !== source) writeFileSync(file, fixed);
}

console.log(`Generated typed gRPC stubs in ${path.relative(root, outDir)}`);
