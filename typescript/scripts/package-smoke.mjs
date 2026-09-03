import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { access, mkdir, mkdtemp, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

import { chromium } from "@playwright/test";
import { build } from "esbuild";

const exec = promisify(execFile);
const require = createRequire(import.meta.url);
const workspace = dirname(fileURLToPath(new URL("../package.json", import.meta.url)));
const typescriptCompiler = join(
  dirname(require.resolve("typescript/package.json")),
  "bin",
  "tsc",
);
const temporary = await mkdtemp(join(tmpdir(), "tailsurf-package-smoke-"));
const tarballs = join(temporary, "tarballs");
const consumer = join(temporary, "consumer");

try {
  await Promise.all([mkdir(tarballs), mkdir(consumer)]);
  const protocolTarball = await pack("protocol");
  const clientTarball = await pack("client");
  const aliasTarball = await pack("tailsurf");

  await writeFile(
    join(consumer, "package.json"),
    `${JSON.stringify({ name: "tailsurf-package-smoke", private: true, type: "module" }, null, 2)}\n`,
  );
  await run("npm", [
    "install",
    "--ignore-scripts",
    "--no-audit",
    "--no-fund",
    "@types/node@22",
    protocolTarball,
    clientTarball,
    aliasTarball,
  ], consumer);

  const installedProtocol = join(
    consumer,
    "node_modules",
    "@tailsurf",
    "protocol",
  );
  const installedClient = join(
    consumer,
    "node_modules",
    "@tailsurf",
    "client",
  );
  const installedAlias = join(consumer, "node_modules", "tailsurf");
  const [protocolManifest, clientManifest, aliasManifest] = await Promise.all([
    readManifest(installedProtocol),
    readManifest(installedClient),
    readManifest(installedAlias),
  ]);
  for (const manifest of [protocolManifest, clientManifest, aliasManifest]) {
    assert.equal(manifest.private, undefined);
    assert.equal(manifest.engines?.node, ">=22");
  }
  assert.equal(
    clientManifest.dependencies?.["@tailsurf/protocol"],
    `^${protocolManifest.version}`,
  );
  assert.equal(
    aliasManifest.dependencies?.["@tailsurf/client"],
    `^${clientManifest.version}`,
  );
  for (const packageDirectory of [
    installedProtocol,
    installedClient,
    installedAlias,
  ]) {
    await Promise.all(
      ["LICENSE", "README.md"].map((name) =>
        access(join(packageDirectory, name))
      ),
    );
    await assert.rejects(access(join(packageDirectory, "src")));
  }
  await Promise.all([
    access(join(installedProtocol, "dist", "fixtures", "rest-v1.json")),
    access(join(installedProtocol, "dist", "fixtures", "v1.json")),
  ]);
  for (const path of [
    join(installedProtocol, "dist", ".tsbuildinfo"),
    join(installedProtocol, "dist", "link-label.js"),
    join(installedClient, "dist", ".tsbuildinfo"),
  ]) {
    await assert.rejects(access(path));
  }
  for (const packageDirectory of [installedProtocol, installedClient]) {
    assert.equal(
      (await readdir(join(packageDirectory, "dist"))).some((name) =>
        name.endsWith(".map")
      ),
      false,
    );
  }

  await writeFile(join(consumer, "node-smoke.mjs"), `
import assert from "node:assert/strict";
import { TsfClient, parseStreamId } from "@tailsurf/client";
import { TsfClient as AliasTsfClient } from "tailsurf";

const id = parseStreamId("0123456789abcdefghjkmnpqrstvwxyz");
const client = new TsfClient({ apiOrigin: "https://tail.surf" });
assert.equal(id, "0123456789abcdefghjkmnpqrstvwxyz");
assert.equal(client.apiOrigin, "https://tail.surf");
assert.equal(AliasTsfClient, TsfClient);
`);
  await run(process.execPath, ["node-smoke.mjs"], consumer);

  await writeFile(join(consumer, "type-smoke.ts"), `
import {
  TsfClient,
  parseStreamId,
  type ReadRecord,
  type StreamId,
  type WebSocketFactory,
} from "@tailsurf/client";

const streamId: StreamId = parseStreamId("0123456789abcdefghjkmnpqrstvwxyz");
const client = new TsfClient({ apiOrigin: "https://tail.surf" });
const factory: WebSocketFactory = (url, protocol) => new WebSocket(url, protocol);

async function collect(): Promise<readonly ReadRecord[]> {
  const records: ReadRecord[] = [];
  const session = await client.connectReader({ streamId });
  for await (const record of session) {
    records.push(record);
  }
  return records;
}

void factory;
void collect;
`);
  for (const [environment, compilerOptions] of Object.entries({
    browser: {
      lib: ["ES2024", "DOM", "DOM.Iterable"],
      module: "ESNext",
      moduleResolution: "Bundler",
    },
    node: {
      lib: ["ES2024"],
      module: "NodeNext",
      moduleResolution: "NodeNext",
      types: ["node"],
    },
  })) {
    const config = `tsconfig.${environment}.json`;
    await writeFile(join(consumer, config), `${JSON.stringify({
      compilerOptions: {
        ...compilerOptions,
        noEmit: true,
        strict: true,
        target: "ES2022",
      },
      files: ["type-smoke.ts"],
    }, null, 2)}\n`);
    await run(
      process.execPath,
      [typescriptCompiler, "--project", config],
      consumer,
    );
  }

  const bundle = await build({
    bundle: true,
    format: "iife",
    platform: "browser",
    target: "es2022",
    write: false,
    stdin: {
      contents: `
import { TsfClient, parseStreamId } from "@tailsurf/client";

const client = new TsfClient({ apiOrigin: "https://tail.surf" });
globalThis.__tailsurfPackageSmoke = {
  apiOrigin: client.apiOrigin,
  streamId: parseStreamId("0123456789abcdefghjkmnpqrstvwxyz"),
};
`,
      resolveDir: consumer,
      sourcefile: "browser-smoke.js",
    },
  });
  const output = bundle.outputFiles[0];
  assert(output !== undefined);
  const browser = await chromium.launch({ headless: true });
  try {
    const page = await browser.newPage();
    await page.addScriptTag({ content: output.text });
    const result = await page.evaluate(() => globalThis.__tailsurfPackageSmoke);
    assert.deepEqual(result, {
      apiOrigin: "https://tail.surf",
      streamId: "0123456789abcdefghjkmnpqrstvwxyz",
    });
  } finally {
    await browser.close();
  }

  console.log("Packed packages work in Node.js and Chromium.");
} finally {
  await rm(temporary, { force: true, recursive: true });
}

async function pack(packageDirectory) {
  const before = new Set(await readdir(tarballs));
  await run("pnpm", ["pack", "--pack-destination", tarballs], join(workspace, "packages", packageDirectory));
  const created = (await readdir(tarballs)).filter((name) => !before.has(name));
  assert.equal(created.length, 1, `expected one tarball for ${packageDirectory}`);
  return join(tarballs, created[0]);
}

async function readManifest(packageDirectory) {
  return JSON.parse(await readFile(join(packageDirectory, "package.json"), "utf8"));
}

async function run(command, arguments_, cwd) {
  try {
    await exec(command, arguments_, { cwd });
  } catch (error) {
    if (error !== null && typeof error === "object") {
      if ("stdout" in error && error.stdout) {
        process.stdout.write(String(error.stdout));
      }
      if ("stderr" in error && error.stderr) {
        process.stderr.write(String(error.stderr));
      }
    }
    throw error;
  }
}
