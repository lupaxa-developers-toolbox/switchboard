"""POST one JSON body through the webhook sample gateway."""

from __future__ import annotations

import os
import urllib.request

URL = os.environ.get("SWITCHBOARD_POST_URL", "http://127.0.0.1:18081/events")


def main() -> None:
    request = urllib.request.Request(
        URL,
        data=b'{"event":"ping"}',
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=5) as response:
        print(response.status)


if __name__ == "__main__":
    main()
