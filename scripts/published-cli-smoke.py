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


DEFAULT_PACKAGE = "tailsurf-cli"
INSTALL_TIMEOUT_SECS = int(os.environ.get("TSF_PUBLISHED_CLI_INSTALL_TIMEOUT_SECS", "300"))
COMMAND_TIMEOUT_SECS = int(os.environ.get("TSF_PUBLISHED_CLI_COMMAND_TIMEOUT_SECS", "30"))


@dataclass(frozen=True)
class CreatedStream:
    owner_url: str
    write_url: str
    read_url: str


def main() -> int:
    if sys.argv[1:] == ["--self-test"]:
        return self_test()

    args = parse_args()
    require(args.api_url, "TSF_API_URL or --api-url")
    require(args.web_url, "TSF_WEB_URL or --web-url")

    with tempfile.TemporaryDirectory(prefix="tsf-published-cli-") as temp_dir:
        tsf_bin = args.tsf_bin or install_published_cli(args, pathlib.Path(temp_dir))
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
        finally:
            run_tsf(
                tsf_bin,
                args.api_url,
                args.web_url,
                ["delete", created.owner_url],
                "tsf delete cleanup",
                expect_success=False,
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
    parser.add_argument("--package", default=os.environ.get("TSF_CLI_PACKAGE", DEFAULT_PACKAGE))
    parser.add_argument("--cargo", default=os.environ.get("CARGO", "cargo"))
    parser.add_argument("--tsf-bin", default=os.environ.get("TSF_BIN"), help="Use an existing tsf binary instead of cargo installing from crates.")
    parser.add_argument("--registry", default=os.environ.get("TSF_CARGO_REGISTRY"), help="Optional cargo registry name.")
    parser.add_argument("--index", default=os.environ.get("TSF_CARGO_INDEX"), help="Optional cargo registry index URL.")
    parser.add_argument(
        "--locked",
        action="store_true",
        default=env_enabled("TSF_CARGO_INSTALL_LOCKED"),
        help="Pass --locked to cargo install.",
    )
    return parser.parse_args()


def workspace_version() -> str:
    cargo_toml = pathlib.Path(__file__).resolve().parents[1] / "Cargo.toml"
    with cargo_toml.open("rb") as file:
        manifest = tomllib.load(file)
    return str(manifest["workspace"]["package"]["version"])


def install_published_cli(args: argparse.Namespace, temp_dir: pathlib.Path) -> str:
    install_root = temp_dir / "install"
    command = [
        args.cargo,
        "install",
        args.package,
        "--version",
        args.version,
        "--root",
        str(install_root),
        "--force",
    ]
    if args.locked:
        command.append("--locked")
    if args.registry:
        command.extend(["--registry", args.registry])
    if args.index:
        command.extend(["--index", args.index])
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
        ["new", "--format", "json"],
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


def env_enabled(name: str) -> bool:
    return os.environ.get(name, "").lower() in {"1", "true", "yes"}


def redact(text: str) -> str:
    text = re.sub(r"(https?://[^\s#'\"]+/s/[^\s#'\"]+)#[^\s'\"]+", r"\1#<redacted>", text)
    text = re.sub(r"Bearer\s+[^\s]+", "Bearer <redacted>", text)
    return text


def self_test() -> int:
    redacted = redact(
        "failed https://tail.surf/s/0123456789abcdefghjkmnpqrstvwxyz#w=secret Bearer another-secret"
    )
    if "secret" in redacted or "another-secret" in redacted:
        raise PublishedCliSmokeError(f"redaction self-test leaked a secret: {redacted}")
    created = parse_created_stream(
        json.dumps(
            {
                "urls": {
                    "o": "https://tail.surf/s/0123456789abcdefghjkmnpqrstvwxyz#o=owner",
                    "w": "https://tail.surf/s/0123456789abcdefghjkmnpqrstvwxyz#w=write",
                    "r": "https://tail.surf/s/0123456789abcdefghjkmnpqrstvwxyz#r=read",
                }
            }
        ).encode()
    )
    if "#o=" not in created.owner_url or "#w=" not in created.write_url or "#r=" not in created.read_url:
        raise PublishedCliSmokeError("created stream parser self-test failed")
    try:
        parse_created_stream(b'{"urls": {"o": "owner"}}')
    except PublishedCliSmokeError as error:
        if "owner, write, and read URLs" not in str(error):
            raise
    else:
        raise PublishedCliSmokeError("parser self-test did not reject missing URLs")
    return 0


class PublishedCliSmokeError(RuntimeError):
    pass


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"published CLI smoke failed: {redact(str(error))}", file=sys.stderr)
        raise SystemExit(1)
