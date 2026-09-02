import { cp } from "node:fs/promises";

await cp(
  new URL("../../rust/fixtures/", import.meta.url),
  new URL("../packages/protocol/dist/fixtures/", import.meta.url),
  { recursive: true },
);
