from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path


def main() -> None:
    streamlit_executable = shutil.which("streamlit") or "streamlit"
    app_path = Path(__file__).with_name("streamlit_app.py").resolve()
    forwarded_args = list(sys.argv[1:])
    if not any(arg.startswith("--server.fileWatcherType") for arg in forwarded_args):
        forwarded_args.extend(["--server.fileWatcherType", "poll"])
    try:
        completed = subprocess.run([streamlit_executable, "run", str(app_path), *forwarded_args])
    except KeyboardInterrupt:
        raise SystemExit(130)
    raise SystemExit(completed.returncode)
