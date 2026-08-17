import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";

const rustFixtures = new URL("../../rust/fixtures/", import.meta.url);
const typescriptFixtures = new URL("../packages/protocol/fixtures/", import.meta.url);

for (const name of ["rest-v1.json", "v1.json"]) {
  const [rust, typescript] = await Promise.all([
    readFile(new URL(name, rustFixtures)),
    readFile(new URL(name, typescriptFixtures)),
  ]);
  assert.deepEqual(
    typescript,
    rust,
    `${name} differs between the Rust and TypeScript protocol packages`,
  );
}

console.log("Rust and TypeScript protocol fixtures match.");
