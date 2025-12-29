"""
Discover latest stable LLVM/clang and GCC releases for toolchain builds.
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import os
import re
import sys
import socket
from typing import Iterable
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

LLVM_RELEASE_URL = "https://api.github.com/repos/llvm/llvm-project/releases/latest"
GCC_RELEASE_INDEXES = (
    "https://ftp.gnu.org/gnu/gcc/",
    "https://sourceware.org/pub/gcc/releases/",
    "https://mirrors.kernel.org/gnu/gcc/",
)
GCC_GITHUB_LATEST = "https://api.github.com/repos/gcc-mirror/gcc/releases/latest"
GCC_OFFICIAL_RELEASES_PAGE = "https://gcc.gnu.org/releases.html"


@dataclasses.dataclass
class Versions:
    llvm: str
    gcc: str


def _fetch(url: str, headers: dict[str, str], *, timeout: float = 15.0, retries: int = 3) -> str:
    last_exc: Exception | None = None
    for _ in range(retries):
        try:
            req = Request(url, headers=headers)
            with urlopen(req, timeout=timeout) as resp:  # type: ignore[no-untyped-call]
                return resp.read().decode("utf-8")
        except (HTTPError, URLError, TimeoutError, socket.timeout) as exc:  # type: ignore[attr-defined]
            last_exc = exc
    raise RuntimeError(f"Failed to fetch {url}: {last_exc}")


def _parse_semver(version: str) -> tuple[int, ...]:
    return tuple(int(piece) for piece in version.split("."))


def latest_llvm(token: str | None) -> str:
    """Return the latest LLVM/clang tag without the llvmorg- prefix."""
    headers = {"Accept": "application/vnd.github+json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    try:
        body = _fetch(LLVM_RELEASE_URL, headers)
    except (HTTPError, URLError, socket.timeout) as exc:  # pragma: no cover - network errors
        raise RuntimeError(f"Failed to fetch LLVM release info: {exc}") from exc
    data = json.loads(body)
    tag = data.get("tag_name")
    if not tag:
        raise RuntimeError("LLVM release payload missing tag_name")
    return tag.removeprefix("llvmorg-")


def latest_gcc() -> str:
    """Return the latest GCC version using GitHub mirror, official releases page, then mirrors."""
    # Try GitHub mirror releases first (fast, often cached)
    try:
        body = _fetch(GCC_GITHUB_LATEST, {"Accept": "application/vnd.github+json"}, timeout=15.0, retries=2)
        tag = json.loads(body).get("tag_name")
        if tag:
            # tag names in gcc-mirror are usually like gcc-14.2.0
            m = re.match(r"gcc-(\d+\.\d+\.\d+)", tag)
            if m:
                return m.group(1)
    except Exception:
        # Ignore and continue to mirrors
        pass

    # Try the official releases page (simple HTML scrape, usually lightweight)
    try:
        body = _fetch(GCC_OFFICIAL_RELEASES_PAGE, {}, timeout=20.0, retries=3)
        # First numeric release like 15.2.0 or 15.2
        m = re.search(r">GCC (\d+\.\d+(?:\.\d+)?)<", body)
        if m:
            return m.group(1)
    except Exception:
        # Ignore and continue to mirrors
        pass

    errors = []
    for idx in GCC_RELEASE_INDEXES:
        try:
            body = _fetch(idx, {}, timeout=20.0, retries=4)
            matches = re.findall(r"gcc-(\d+\.\d+\.\d+)/", body)
            if not matches:
                errors.append(f"{idx}: no releases found")
                continue
            versions = sorted({_parse_semver(v): v for v in matches}, key=lambda item: item[0])
            return versions[-1][1]
        except (RuntimeError, HTTPError, URLError, socket.timeout) as exc:  # pragma: no cover - network errors
            errors.append(f"{idx}: {exc}")
    raise RuntimeError(f"Failed to fetch GCC release index from mirrors: {'; '.join(errors)}")


def render_env(versions: Versions) -> str:
    """Render versions as KEY=VALUE lines for shell eval."""
    lines: Iterable[str] = (
        f"LLVM_CLANG_TAG={versions.llvm}",
    )
    return "\n".join(lines)


def parse_args(argv: list[str]) -> argparse.Namespace:
    """Parse CLI arguments for the helper."""
    parser = argparse.ArgumentParser(description="Discover latest compiler versions")
    parser.add_argument("--format", choices=["env", "json"], default="env")
    parser.add_argument("--github-token", default=os.getenv("GITHUB_TOKEN"))
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    """Entrypoint for command-line execution."""
    args = parse_args(argv)
    llvm_tag = latest_llvm(args.github_token)
    # GCC version is now fixed in prepare-latest script; only get LLVM
    versions = Versions(llvm=llvm_tag, gcc="")
    if args.format == "env":
        print(render_env(versions))
    else:
        print(json.dumps(dataclasses.asdict(versions)))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
