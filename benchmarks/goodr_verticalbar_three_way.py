"""Run the Goodr Vertical Bar application-query performance suite.

The data loader and recovered verticalbar-mvp SQL live in the parent Dendrites
benchmark workspace. This versioned entrypoint keeps the ocpm-engine regression
case and baseline discoverable from this repository.
"""

from __future__ import annotations

import argparse
import runpy
import sys
from pathlib import Path

DEFAULT_DRIVER = (
    Path(__file__).resolve().parents[2]
    / "compare"
    / "benchmark_goodr_verticalbar_three_way.py"
)


def main() -> None:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--driver", type=Path, default=DEFAULT_DRIVER)
    args, remaining = parser.parse_known_args()
    driver = args.driver.resolve()
    if not driver.is_file():
        raise SystemExit(
            "Vertical Bar Goodr driver not found. Pass --driver pointing to "
            "dendrites/impl/compare/benchmark_goodr_verticalbar_three_way.py"
        )
    sys.argv = [str(driver), *remaining]
    runpy.run_path(str(driver), run_name="__main__")


if __name__ == "__main__":
    main()
