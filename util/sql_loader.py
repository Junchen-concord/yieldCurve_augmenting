from pathlib import Path


def read_sql_file(path: str | Path) -> str:
    return Path(path).read_text(encoding="utf-8")

