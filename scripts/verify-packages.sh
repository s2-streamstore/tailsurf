#!/usr/bin/env bash

set -euo pipefail

package_args=(--workspace --no-verify --locked)
if [[ "${1:-}" == "--allow-dirty" ]]; then
  package_args+=(--allow-dirty)
  shift
fi
if [[ "$#" -ne 0 ]]; then
  echo "usage: $0 [--allow-dirty]" >&2
  exit 2
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
temp_parent="$(cd "${TMPDIR:-/tmp}" && pwd)"
work_dir="$(mktemp -d "$temp_parent/tailsurf-package-verify.XXXXXX")"

cleanup() {
  case "$work_dir" in
    "$temp_parent"/tailsurf-package-verify.*) rm -rf -- "$work_dir" ;;
    *) echo "refusing to remove unexpected package verification path: $work_dir" >&2 ;;
  esac
}
trap cleanup EXIT

cargo_bin="${CARGO:-cargo}"
package_target="$work_dir/package-target"
verify_root="$work_dir/packages"

(
  cd "$repo_root"
  CARGO_TARGET_DIR="$package_target" "$cargo_bin" package "${package_args[@]}"
)

shopt -s nullglob
sdk_archives=("$package_target/package"/tailsurf-[0-9]*.crate)
cli_archives=("$package_target/package"/tailsurf-cli-*.crate)
shopt -u nullglob
if [[ "${#sdk_archives[@]}" -ne 1 || "${#cli_archives[@]}" -ne 1 ]]; then
  echo "expected exactly one packaged tailsurf SDK and CLI archive" >&2
  exit 1
fi

mkdir -p "$verify_root"
tar -xzf "${sdk_archives[0]}" -C "$verify_root"
tar -xzf "${cli_archives[0]}" -C "$verify_root"
sdk_name="$(basename "${sdk_archives[0]}" .crate)"
cli_name="$(basename "${cli_archives[0]}" .crate)"

cat > "$verify_root/Cargo.toml" <<EOF
[workspace]
members = ["$sdk_name", "$cli_name"]
resolver = "3"

[patch.crates-io]
tailsurf = { path = "$sdk_name" }
EOF

verify_target="$work_dir/verify-target"
CARGO_TARGET_DIR="$verify_target" "$cargo_bin" generate-lockfile --manifest-path "$verify_root/Cargo.toml"
CARGO_TARGET_DIR="$verify_target" "$cargo_bin" check --manifest-path "$verify_root/Cargo.toml" --workspace --all-targets --locked
