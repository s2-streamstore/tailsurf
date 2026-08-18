import { ProtocolError } from "./errors.js";

export type LinkPermissions = "o" | "r" | "w" | "rw";

export function parseLinkPermissions(input: string): LinkPermissions {
  if (input.length === 0) {
    throw new ProtocolError("empty_permissions", "permission string cannot be empty");
  }

  const seen = new Set<string>();
  for (const permission of input) {
    if (permission !== "o" && permission !== "r" && permission !== "w") {
      throw new ProtocolError(
        "unknown_permission",
        `unknown stream permission ${JSON.stringify(permission)}`,
      );
    }
    if (seen.has(permission)) {
      throw new ProtocolError(
        "duplicate_permission",
        `duplicate stream permission ${JSON.stringify(permission)}`,
      );
    }
    seen.add(permission);
  }

  if (seen.has("o")) {
    if (seen.size !== 1) {
      throw new ProtocolError(
        "owner_permissions_combined",
        "owner permission already includes read and write",
      );
    }
    return "o";
  }
  if (seen.has("r") && seen.has("w")) {
    return "rw";
  }
  if (seen.has("r")) {
    return "r";
  }
  return "w";
}

export function permissionsAllowOwner(permissions: LinkPermissions): boolean {
  return permissions === "o";
}

export function permissionsAllowRead(permissions: LinkPermissions): boolean {
  return permissions === "o" || permissions.includes("r");
}

export function permissionsAllowWrite(permissions: LinkPermissions): boolean {
  return permissions === "o" || permissions.includes("w");
}
