"""Encode and decode the profile sample without starting the gateway."""

from __future__ import annotations

import json
from pathlib import Path

from lupaxa.switchboard.config import load_config
from lupaxa.switchboard.profile import decode_profile, encode_profile


def main() -> None:
    root = Path(__file__).resolve().parent
    config = load_config(root / "switchboard.yml", user_path=None)
    profile = config.profiles["device"]
    message = (root / "message.json").read_text(encoding="utf-8")
    raw = encode_profile(profile, message)
    decoded = json.loads(decode_profile(profile, raw))
    print(json.dumps({"decoded": decoded, "bytes": len(raw)}))


if __name__ == "__main__":
    main()
