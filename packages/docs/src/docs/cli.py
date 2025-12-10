"""CLI for documentation site."""

import subprocess
import sys
from pathlib import Path


def main() -> int:
    """Define main CLI entry point."""
    docs_dir = Path(__file__).parent.parent.parent

    if len(sys.argv) > 1:
        command = sys.argv[1]
    else:
        command = "serve"

    if command == "serve":
        return subprocess.call(["mkdocs", "serve"], cwd=docs_dir)
    if command == "build":
        return subprocess.call(["mkdocs", "build"], cwd=docs_dir)
    if command == "deploy":
        return subprocess.call(["mkdocs", "gh-deploy"], cwd=docs_dir)

    print(f"Unknown command: {command}")
    print("Available commands: serve, build, deploy")
    return 1


if __name__ == "__main__":
    sys.exit(main())
