#!/usr/bin/env python3
"""Patch cyclors 0.2.7 in a dedicated build-only CARGO_HOME after cargo fetch.

Never use a shared/user Cargo cache. Keeps fortification enabled and corrects
the remaining-buffer length used by the DDS configuration printer.
"""
import hashlib
from pathlib import Path
import sys


ORIGINAL = "b4500d5a6bb1b12fdb4ca546eaf62762146db33551ff3b70e1fdcd560e216479"
BEFORE = "snprintf (resp, 256,"
AFTER = "snprintf (resp, sizeof (res) - (size_t) (resp - res),"


def main():
    root = Path(sys.argv[1]).resolve()
    matches = list(root.glob(
        "registry/src/*/cyclors-0.2.7/cyclonedds/src/core/ddsi/src/ddsi_config.c"))
    if len(matches) != 1:
        raise SystemExit("Expected exactly one fetched cyclors 0.2.7 source")
    source = matches[0]
    content = source.read_text()
    original = content.replace(AFTER, BEFORE)
    if hashlib.sha256(original.encode()).hexdigest() != ORIGINAL:
        raise SystemExit("Unsupported DDS source hash; refusing to patch")
    if content.count(AFTER) == 2 and BEFORE not in content:
        print("DDS configuration buffer fix already applied")
        return
    if content.count(BEFORE) != 2 or AFTER in content:
        raise SystemExit("Unexpected snprintf sites; refusing to patch")
    source.write_text(content.replace(BEFORE, AFTER))
    print(f"Patched exactly two remaining-buffer lengths in {source}")


if __name__ == "__main__":
    main()
