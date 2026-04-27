from pathlib import Path
import sys


ROOT_DIR = Path(__file__).resolve().parent
SRC_DIR = ROOT_DIR / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from retail_pipeline.pipeline import run_pipeline


if __name__ == "__main__":
    run_pipeline(project_root=ROOT_DIR)

