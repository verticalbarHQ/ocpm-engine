#!/usr/bin/env python3
"""Deterministic one-shot worker for the candidate-gate protocol."""

from __future__ import annotations

import json
import sys
import time


def main() -> None:
    request = json.loads(sys.stdin.readline())
    payload = request["payload"]
    retained = bytearray(payload["memory_kib"] * 1024)
    for offset in range(0, len(retained), 4096):
        retained[offset] = offset % 251
    time.sleep(payload["sleep_ms"] / 1000)
    print(
        json.dumps(
            {
                "input": {
                    "workload": request["workload"],
                    "values": payload["values"],
                },
                "answer": {
                    "count": len(payload["values"]),
                    "sum": sum(payload["values"]),
                },
                "storage_bytes": payload["storage_bytes"],
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
