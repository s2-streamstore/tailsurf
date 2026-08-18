import { describe, expect, it } from "vitest";

import {
  buildStreamLink,
  generateStreamId,
  generateClientWriterId,
  parseStreamId,
  parseStreamUrl,
  parseLinkId,
  parseLinkPermissions,
  permissionsAllowOwner,
  permissionsAllowRead,
  permissionsAllowWrite,
  ProtocolError,
  WRITER_ID_BYTE_LENGTH,
} from "../src/index.js";

const STREAM_ID = parseStreamId("0123456789abcdefghjkmnpqrstvwxyz");
const OWNER_SECRET = "A".repeat(32);
const WRITE_SECRET = "b".repeat(32);

describe("IDs", () => {
  it("canonicalizes stream UBIDs", () => {
    expect(parseStreamId("O123456789ABCDEFGHJKMNPQRSTVWXYZ")).toBe(
      "0123456789abcdefghjkmnpqrstvwxyz",
    );
  });

  it("generates valid stream and writer IDs", () => {
    expect(parseStreamId(generateStreamId())).toHaveLength(32);
    expect(generateClientWriterId()).toHaveLength(WRITER_ID_BYTE_LENGTH);
  });

  it("accepts canonical human-readable Link IDs", () => {
    expect(parseLinkId("owner")).toBe("owner");
    expect(parseLinkId("deploy-bot")).toBe("deploy-bot");
    expect(parseLinkId("a".repeat(64))).toBe("a".repeat(64));
  });

  it("rejects malformed IDs", () => {
    expect(() => parseStreamId("short")).toThrow(ProtocolError);
    for (const id of ["", "Owner", "-owner", "owner-", "deploy_bot", "a".repeat(65)]) {
      expect(() => parseLinkId(id)).toThrow(ProtocolError);
    }
  });
});

describe("link permissions", () => {
  it("canonicalizes read/write and applies owner implications", () => {
    expect(parseLinkPermissions("wr")).toBe("rw");
    expect(permissionsAllowOwner("o")).toBe(true);
    expect(permissionsAllowRead("o")).toBe(true);
    expect(permissionsAllowWrite("o")).toBe(true);
    expect(permissionsAllowWrite("r")).toBe(false);
  });

  it.each(["", "rx", "rr", "or", "ow", "orw"])(
    "rejects invalid permissions %j",
    (permissions) => {
      expect(() => parseLinkPermissions(permissions)).toThrow(ProtocolError);
    },
  );
});

describe("stream URLs", () => {
  it("parses fragment credentials without exposing them to the server", () => {
    expect(
      parseStreamUrl(
        `https://tail.surf/s/${STREAM_ID}#o=${OWNER_SECRET}`,
      ),
    ).toEqual({
      streamId: STREAM_ID,
      link: { declaredPermissions: "o", secret: OWNER_SECRET },
    });
  });

  it("parses a sequence anchor from the client-only fragment", () => {
    expect(
      parseStreamUrl(
        `https://tail.surf/s/${STREAM_ID}#r=${WRITE_SECRET}&at=1234`,
      ),
    ).toEqual({
      streamId: STREAM_ID,
      link: { declaredPermissions: "r", secret: WRITE_SECRET },
      anchor: { seqNum: 1234n },
    });
    expect(parseStreamUrl(`https://tail.surf/s/${STREAM_ID}#at=0`)).toEqual({
      streamId: STREAM_ID,
      anchor: { seqNum: 0n },
    });
  });

  it.each(["", "-1", "01", "12a", "1.5", "9007199254740992"])(
    "rejects invalid sequence anchor %j",
    (at) => {
      expect(() =>
        parseStreamUrl(`https://tail.surf/s/${STREAM_ID}#at=${at}`)
      ).toThrow(ProtocolError);
    },
  );

  it("builds a canonical stream link and removes the base query", () => {
    expect(
      buildStreamLink(
        "http://user:password@localhost:3000/?ignored=true",
        STREAM_ID,
        "rw",
        WRITE_SECRET,
      ).toString(),
    ).toBe(`http://localhost:3000/s/${STREAM_ID}#rw=${WRITE_SECRET}`);
    expect(
      buildStreamLink(
        "https://tail.surf",
        STREAM_ID,
        "r",
        WRITE_SECRET,
        { seqNum: 50n },
      ).toString(),
    ).toBe(`https://tail.surf/s/${STREAM_ID}#r=${WRITE_SECRET}&at=50`);
  });

  it.each([
    "https://tail.surf/not-a-stream",
    `https://tail.surf/s/${STREAM_ID}/`,
    `https://tail.surf/s/${STREAM_ID}#x=secret`,
    `https://tail.surf/s/${STREAM_ID}#r=`,
    `https://tail.surf/s/${STREAM_ID}#r=read&w=write`,
    `https://tail.surf/s/${STREAM_ID}#at=1&at=2`,
    `https://tail.surf/s/${STREAM_ID}#unknown=value`,
    `https://tail.surf/s/${STREAM_ID}#r=${WRITE_SECRET}&at=1&at=2`,
    `https://tail.surf/s/${STREAM_ID}?at=50`,
    `https://tail.surf/s/${STREAM_ID}?seq_num=100#at=50`,
    `https://tail.surf/s/${STREAM_ID}?view=raw`,
    `https://tail.surf/s/${STREAM_ID}?`,
    `https://tail.surf/s/${STREAM_ID}#`,
    `https://tail.surf/s/${STREAM_ID}#&`,
    `https://tail.surf/s/${STREAM_ID}#at=1&`,
    `https://tail.surf/s/${STREAM_ID}#w=${"a".repeat(31)}`,
    `https://tail.surf/s/${STREAM_ID}#w=${"a".repeat(33)}`,
    `https://tail.surf/s/${STREAM_ID}#w=${"a".repeat(31)}%27`,
    `file:///s/${STREAM_ID}#w=${WRITE_SECRET}`,
  ])("rejects malformed stream URL %s", (url) => {
    expect(() => parseStreamUrl(url)).toThrow(ProtocolError);
  });
});
