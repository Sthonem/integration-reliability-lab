"""Developer helpers.

    python -m irlab.devtools init-env           # .env from .env.example with fresh random secrets
    python -m irlab.devtools init-env --force   # overwrite an existing .env
"""

import argparse
import secrets
import sys
from pathlib import Path

PLACEHOLDER = "__generate__"


def init_env(example: Path, target: Path, *, force: bool = False) -> int:
    if target.exists() and not force:
        print(f"{target} already exists; refusing to overwrite (use --force).", file=sys.stderr)
        return 1
    if not example.exists():
        print(f"{example} not found.", file=sys.stderr)
        return 1
    lines = []
    generated = 0
    for line in example.read_text().splitlines():
        while PLACEHOLDER in line:
            line = line.replace(PLACEHOLDER, secrets.token_urlsafe(32), 1)
            generated += 1
        lines.append(line)
    target.write_text("\n".join(lines) + "\n")
    target.chmod(0o600)
    # Never print the secrets themselves.
    print(f"Wrote {target} with {generated} freshly generated secret(s).")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m irlab.devtools")
    sub = parser.add_subparsers(dest="command", required=True)
    p_init = sub.add_parser("init-env", help="create .env with random webhook secrets")
    p_init.add_argument("--example", type=Path, default=Path(".env.example"))
    p_init.add_argument("--target", type=Path, default=Path(".env"))
    p_init.add_argument("--force", action="store_true")
    args = parser.parse_args(argv)
    if args.command == "init-env":
        return init_env(args.example, args.target, force=args.force)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
