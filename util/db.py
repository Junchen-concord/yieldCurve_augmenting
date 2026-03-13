from pathlib import Path
from urllib.parse import quote_plus

from dotenv import dotenv_values
from sqlalchemy import create_engine


def _pick_env(cfg: dict, keys: list[str]):
    for key in keys:
        value = cfg.get(key)
        if value not in (None, ""):
            return value
    return None


def create_db_engine(env_path: str | Path = ".env"):
    env_file = Path(env_path)
    cfg = dotenv_values(env_file)

    server = _pick_env(cfg, ["DB_SERVER", "SQL_SERVER", "SERVER", "HOST"])
    database = _pick_env(cfg, ["DB_DATABASE", "SQL_DATABASE", "DATABASE", "DB_NAME"])
    username = _pick_env(cfg, ["DB_USERNAME", "SQL_USERNAME", "USERNAME", "USER"])
    password = _pick_env(cfg, ["DB_PASSWORD", "SQL_PASSWORD", "PASSWORD", "PASS"])
    driver = _pick_env(cfg, ["DB_DRIVER", "SQL_DRIVER"]) or "ODBC Driver 18 for SQL Server"

    if not all([server, database, username, password]):
        missing = [
            name
            for name, val in {
                "server": server,
                "database": database,
                "username": username,
                "password": password,
            }.items()
            if not val
        ]
        raise ValueError(f"Missing required DB values in .env: {', '.join(missing)}")

    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
        "Encrypt=yes;TrustServerCertificate=yes;"
    )
    return create_engine(f"mssql+pyodbc:///?odbc_connect={quote_plus(conn_str)}")

