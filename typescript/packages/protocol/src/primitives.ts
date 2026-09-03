export const U64_PATTERN = /^(?:0|[1-9][0-9]{0,19})$/;
export const MAX_U64 = 0xffff_ffff_ffff_ffffn;
export const MAX_SAFE_INTEGER_U64 = BigInt(Number.MAX_SAFE_INTEGER);
export const MAX_PART_INDEX = 0x7fff_ffff;

export function encodeBase64url(bytes: Uint8Array): string {
  let binary = "";
  for (let offset = 0; offset < bytes.byteLength; offset += 0x8000) {
    binary += String.fromCharCode(...bytes.subarray(offset, offset + 0x8000));
  }
  return btoa(binary).replaceAll("+", "-").replaceAll("/", "_").replace(/=+$/, "");
}

export function decodeBase64url(value: string): Uint8Array {
  const standard = value.replaceAll("-", "+").replaceAll("_", "/");
  const decoded = atob(standard + "=".repeat((4 - standard.length % 4) % 4));
  return Uint8Array.from(decoded, (character) => character.charCodeAt(0));
}

export function tryParseDecimalU64(value: string): bigint | undefined {
  if (!U64_PATTERN.test(value)) {
    return undefined;
  }
  const parsed = BigInt(value);
  return parsed > MAX_U64 ? undefined : parsed;
}

export function canonicalBase64url(value: string, expectedBytes?: number): boolean {
  try {
    const decoded = decodeBase64url(value);
    return (expectedBytes === undefined || decoded.byteLength === expectedBytes) &&
      encodeBase64url(decoded) === value;
  } catch {
    return false;
  }
}
