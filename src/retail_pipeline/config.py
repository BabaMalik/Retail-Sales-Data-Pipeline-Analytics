from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class PipelinePaths:
    project_root: Path

    @property
    def raw_dir(self) -> Path:
        return self.project_root / "data" / "raw"

    @property
    def output_dir(self) -> Path:
        return self.project_root / "output" / "lakehouse"

    @property
    def bronze_dir(self) -> Path:
        return self.output_dir / "bronze"

    @property
    def silver_dir(self) -> Path:
        return self.output_dir / "silver"

    @property
    def gold_dir(self) -> Path:
        return self.output_dir / "gold"

    @property
    def reports_dir(self) -> Path:
        return self.project_root / "output" / "reports"

    def bronze_table_path(self, table_name: str) -> str:
        return str(self.bronze_dir / table_name)

    def silver_table_path(self, table_name: str) -> str:
        return str(self.silver_dir / table_name)

    def gold_table_path(self, table_name: str) -> str:
        return str(self.gold_dir / table_name)

