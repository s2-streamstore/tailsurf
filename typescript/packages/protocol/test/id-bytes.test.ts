import { describe, expect, it } from "vitest";

import {
  ProtocolError,
  streamIdFromBytes,
} from "../src/index.js";

describe("binary UBIDs", () => {
  it("rejects values with the wrong length", () => {
    expect(() => streamIdFromBytes(new Uint8Array(19))).toThrow(ProtocolError);
  });
});
