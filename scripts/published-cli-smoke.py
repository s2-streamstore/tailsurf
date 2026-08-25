#!/usr/bin/env python3
"""Install the published tailsurf CLI and smoke it against a Tailsurf API."""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import re
import subprocess
import sys
import tempfile
import time
import tomllib
from dataclasses import dataclass


INSTALL_TIMEOUT_SECS = int(os.environ.get("TSF_PUBLISHED_CLI_INSTALL_TIMEOUT_SECS", "300"))
COMMAND_TIMEOUT_SECS = int(os.environ.get("TSF_PUBLISHED_CLI_COMMAND_TIMEOUT_SECS", "30"))


@dataclass(frozen=True)
class CreatedStream:
    owner_link: str
    write_link: str
    read_link: str


def main() -> int:
    if sys.argv[1:] == ["--self-test"]:
        self_test()
        return 0

    args = parse_args()
    require(args.origin, "TSF_ORIGIN or --origin")

    with tempfile.TemporaryDirectory(prefix="tsf-published-cli-") as temp_dir:
        tsf_bin = install_published_cli(args.version, pathlib.Path(temp_dir))
        run_command([tsf_bin, "--help"], "tsf --help")
        created = create_stream(tsf_bin, args.origin)
        message = f"tailsurf published cli smoke {int(time.time())}\n".encode()
        try:
            run_tsf(
                tsf_bin,
                args.origin,
                ["write", created.write_link],
                "tsf write",
                input_data=message,
            )
            replayed = run_tsf(
                tsf_bin,
                args.origin,
                ["replay", created.read_link],
                "tsf replay",
            )
            if message not in replayed.stdout:
                raise PublishedCliSmokeError("tsf replay did not include the smoke record")
        except Exception:
            run_tsf(
                tsf_bin,
                args.origin,
                ["delete", created.owner_link, "--yes"],
                "tsf delete cleanup",
                expect_success=False,
            )
            raise
        run_tsf(
            tsf_bin,
            args.origin,
            ["delete", created.owner_link, "--yes"],
            "tsf delete",
        )

    print("published CLI smoke passed")
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Install tailsurf-cli from crates.io and run new/write/replay/delete against a Tailsurf API."
    )
    parser.add_argument("--origin", default=os.environ.get("TSF_ORIGIN"))
    parser.add_argument("--version", default=os.environ.get("TSF_CLI_VERSION") or workspace_version())
    return parser.parse_args()


def workspace_version() -> str:
    cargo_toml = pathlib.Path(__file__).resolve().parents[1] / "Cargo.toml"
    with cargo_toml.open("rb") as file:
        manifest = tomllib.load(file)
    return str(manifest["workspace"]["package"]["version"])


def install_published_cli(version: str, temp_dir: pathlib.Path) -> str:
    install_root = temp_dir / "install"
    command = [
        "cargo",
        "install",
        "tailsurf-cli",
        "--version",
        version,
        "--root",
        str(install_root),
        "--locked",
    ]
    run_command(command, "cargo install published tailsurf CLI", timeout=INSTALL_TIMEOUT_SECS)
    tsf_bin = install_root / "bin" / ("tsf.exe" if os.name == "nt" else "tsf")
    if not tsf_bin.exists():
        raise PublishedCliSmokeError(f"cargo install did not create {tsf_bin}")
    return str(tsf_bin)


def create_stream(tsf_bin: str, origin: str) -> CreatedStream:
    result = run_tsf(
        tsf_bin,
        origin,
        [
            "new",
            "--json",
            "--link",
            "owner=Smoke owner",
            "--link",
            "write=Smoke writer",
            "--link",
            "read=Smoke reader",
        ],
        "tsf new --json",
    )
    return parse_created_stream(result.stdout)


def run_tsf(
    tsf_bin: str,
    origin: str,
    args: list[str],
    label: str,
    *,
    input_data: bytes | None = None,
    expect_success: bool = True,
) -> subprocess.CompletedProcess[bytes]:
    return run_command(
        [tsf_bin, "--origin", origin, *args],
        label,
        input_data=input_data,
        expect_success=expect_success,
    )


def run_command(
    command: list[str],
    label: str,
    *,
    input_data: bytes | None = None,
    expect_success: bool = True,
    timeout: int = COMMAND_TIMEOUT_SECS,
) -> subprocess.CompletedProcess[bytes]:
    result = subprocess.run(
        command,
        input=input_data,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=timeout,
        check=False,
    )
    if expect_success and result.returncode != 0:
        raise PublishedCliSmokeError(
            f"{label} failed with exit {result.returncode}\n"
            f"stdout={redact(result.stdout.decode(errors='replace'))}\n"
            f"stderr={redact(result.stderr.decode(errors='replace'))}"
        )
    return result


def parse_created_stream(stdout: bytes) -> CreatedStream:
    try:
        payload = json.loads(stdout.decode())
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise PublishedCliSmokeError("tsf new did not return valid JSON") from error
    if not isinstance(payload, dict):
        raise PublishedCliSmokeError("tsf new JSON was not an object")
    links = payload.get("links")
    if not isinstance(links, list):
        raise PublishedCliSmokeError("tsf new JSON did not contain links")
    links_by_permission = {
        link.get("permissions"): link.get("url")
        for link in links
        if isinstance(link, dict)
    }
    owner_link = links_by_permission.get("owner")
    write_link = links_by_permission.get("write")
    read_link = links_by_permission.get("read")
    if not all(isinstance(link, str) and link for link in [owner_link, write_link, read_link]):
        raise PublishedCliSmokeError("tsf new JSON did not contain owner, write, and read links")
    return CreatedStream(owner_link=owner_link, write_link=write_link, read_link=read_link)


def require(value: str | None, name: str) -> None:
    if not value:
        raise PublishedCliSmokeError(f"{name} must be set")


def redact(text: str) -> str:
    text = re.sub(r"(https?://[^\s#'\"]+/s/[^\s#'\"]+)#[^\s'\"]+", r"\1#<redacted>", text)
    text = re.sub(r"Bearer\s+[^\s]+", "Bearer <redacted>", text)
    return text


def self_test() -> None:
    redacted = redact(
        "https://tailsurf.example/s/stream#o=owner-secret Authorization: Bearer bearer-secret"
    )
    if "owner-secret" in redacted or "bearer-secret" in redacted:
        raise PublishedCliSmokeError("redaction self-test failed")

    payload = b'{"links":[{"permissions":"owner","url":"owner"},{"permissions":"write","url":"write"},{"permissions":"read","url":"read"}]}'
    if parse_created_stream(payload) != CreatedStream("owner", "write", "read"):
        raise PublishedCliSmokeError("stream parsing self-test failed")
    for malformed in (b"not-json", b'{"links":[{"permissions":"owner","url":"owner"}]}'):
        try:
            parse_created_stream(malformed)
        except PublishedCliSmokeError:
            continue
        raise PublishedCliSmokeError("malformed stream output was accepted")

    print("published CLI smoke self-test passed")


class PublishedCliSmokeError(RuntimeError):
    pass


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"published CLI smoke failed: {redact(str(error))}", file=sys.stderr)
        raise SystemExit(1)
