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
    owner_url: str
    write_url: str
    read_url: str


def main() -> int:
    if sys.argv[1:] == ["--self-test"]:
        self_test()
        return 0

    args = parse_args()
    require(args.api_url, "TSF_API_URL or --api-url")
    require(args.web_url, "TSF_WEB_URL or --web-url")

    with tempfile.TemporaryDirectory(prefix="tsf-published-cli-") as temp_dir:
        tsf_bin = install_published_cli(args.version, pathlib.Path(temp_dir))
        run_command([tsf_bin, "--help"], "tsf --help")
        created = create_stream(tsf_bin, args.api_url, args.web_url)
        message = f"tailsurf published cli smoke {int(time.time())}\n".encode()
        try:
            run_tsf(
                tsf_bin,
                args.api_url,
                args.web_url,
                ["write", created.write_url],
                "tsf write",
                input_data=message,
            )
            replayed = run_tsf(
                tsf_bin,
                args.api_url,
                args.web_url,
                ["replay", created.read_url],
                "tsf replay",
            )
            if message not in replayed.stdout:
                raise PublishedCliSmokeError("tsf replay did not include the smoke record")
        except Exception:
            run_tsf(
                tsf_bin,
                args.api_url,
                args.web_url,
                ["delete", created.owner_url],
                "tsf delete cleanup",
                expect_success=False,
            )
            raise
        run_tsf(
            tsf_bin,
            args.api_url,
            args.web_url,
            ["delete", created.owner_url],
            "tsf delete",
        )

    print("published CLI smoke passed")
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Install tailsurf-cli from crates.io and run new/write/replay/delete against a Tailsurf API."
    )
    parser.add_argument("--api-url", default=os.environ.get("TSF_API_URL"))
    parser.add_argument("--web-url", default=os.environ.get("TSF_WEB_URL"))
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


def create_stream(tsf_bin: str, api_url: str, web_url: str) -> CreatedStream:
    result = run_tsf(
        tsf_bin,
        api_url,
        web_url,
        ["new", "--format", "json", "--link", "owner", "--link", "write", "--link", "view"],
        "tsf new --format json",
    )
    return parse_created_stream(result.stdout)


def run_tsf(
    tsf_bin: str,
    api_url: str,
    web_url: str,
    args: list[str],
    label: str,
    *,
    input_data: bytes | None = None,
    expect_success: bool = True,
) -> subprocess.CompletedProcess[bytes]:
    return run_command(
        [tsf_bin, "--api-url", api_url, "--web-url", web_url, *args],
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
    urls = payload.get("urls")
    if not isinstance(urls, dict):
        raise PublishedCliSmokeError("tsf new JSON did not contain urls")
    owner_url = urls.get("o")
    write_url = urls.get("w")
    read_url = urls.get("r")
    if not all(isinstance(url, str) and url for url in [owner_url, write_url, read_url]):
        raise PublishedCliSmokeError("tsf new JSON did not contain owner, write, and read URLs")
    return CreatedStream(owner_url=owner_url, write_url=write_url, read_url=read_url)


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

    payload = b'{"urls":{"o":"owner","w":"write","r":"read"}}'
    if parse_created_stream(payload) != CreatedStream("owner", "write", "read"):
        raise PublishedCliSmokeError("stream parsing self-test failed")
    for malformed in (b"not-json", b'{"urls":{"o":"owner"}}'):
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
