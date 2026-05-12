"""Persist trained projection model runs to versioned folders."""
from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from .projection_stage_a import StageAModel
from .projection_stage_b import StageBModel
from .projection_stage_c import StageCRecoveryModel


@dataclass
class ProjectionModelRun:
    """Loaded model run artifacts ready for inference."""

    run_dir: Path
    metadata: dict
    feature_contract: dict
    metrics: dict
    artifacts: dict
    stage_a: StageAModel
    stage_b: StageBModel
    stage_c: StageCRecoveryModel


def utc_run_tag() -> str:
    """Return a sortable UTC run tag: YYYYMMDDTHHMMSSZ."""
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _jsonable(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return float(value)
    if isinstance(value, (np.bool_,)):
        return bool(value)
    if isinstance(value, np.ndarray):
        return [_jsonable(v) for v in value.tolist()]
    if isinstance(value, pd.DataFrame):
        return [_jsonable(row) for row in value.to_dict(orient="records")]
    if isinstance(value, pd.Series):
        return {str(k): _jsonable(v) for k, v in value.to_dict().items()}
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_jsonable(v) for v in value]
    return str(value)


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_jsonable(payload), indent=2, sort_keys=True), encoding="utf-8")


def read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _git_info(project_root: Path) -> dict:
    def _run(args: list[str]) -> str | None:
        try:
            return subprocess.check_output(args, cwd=project_root, text=True, stderr=subprocess.DEVNULL).strip()
        except Exception:
            return None

    sha = _run(["git", "rev-parse", "HEAD"])
    status = _run(["git", "status", "--short"])
    return {
        "sha": sha,
        "dirty": bool(status),
        "status_short": status,
    }


def persist_projection_run(
    project_root: str | Path,
    stage_a_model,
    stage_b_model,
    stage_c_recovery_model,
    feature_contract: dict,
    metrics: dict | None = None,
    metadata: dict | None = None,
    artifacts: dict | None = None,
    run_tag: str | None = None,
) -> Path:
    """Persist trained Stage A/B/C models and run metadata.

    This intentionally saves model artifacts and compact metadata only. It does
    not save loan-level, installment-level, or payment-level training data.
    """
    root = Path(project_root).resolve()
    tag = run_tag or utc_run_tag()
    run_dir = root / "prediction_models" / "runs" / tag
    if run_dir.exists():
        raise FileExistsError(f"Run directory already exists: {run_dir}")

    (run_dir / "stage_a").mkdir(parents=True)
    (run_dir / "stage_b").mkdir(parents=True)
    (run_dir / "stage_c").mkdir(parents=True)
    (run_dir / "artifacts").mkdir(parents=True)

    stage_a_model.booster.save_model(str(run_dir / "stage_a" / "model.json"))
    stage_b_model.clf.save_model(str(run_dir / "stage_b" / "classifier.txt"))
    stage_b_model.reg.save_model(str(run_dir / "stage_b" / "regressor.txt"))
    stage_c_recovery_model.clf.save_model(str(run_dir / "stage_c" / "classifier.txt"))
    stage_c_recovery_model.reg.save_model(str(run_dir / "stage_c" / "regressor.txt"))

    base_metadata = {
        "run_tag": tag,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "run_dir": str(run_dir),
        "git": _git_info(root),
        "model_files": {
            "stage_a": "stage_a/model.json",
            "stage_b_classifier": "stage_b/classifier.txt",
            "stage_b_regressor": "stage_b/regressor.txt",
            "stage_c_classifier": "stage_c/classifier.txt",
            "stage_c_regressor": "stage_c/regressor.txt",
        },
    }
    if metadata:
        base_metadata.update(metadata)

    write_json(run_dir / "metadata.json", base_metadata)
    write_json(run_dir / "feature_contract.json", feature_contract)
    write_json(run_dir / "metrics.json", metrics or {})
    write_json(run_dir / "artifacts" / "artifacts.json", artifacts or {})

    return run_dir


def _resolve_run_dir(project_root: str | Path, run_tag_or_dir: str | Path) -> Path:
    candidate = Path(run_tag_or_dir)
    if candidate.is_absolute() or candidate.exists():
        return candidate.resolve()
    return (Path(project_root).resolve() / "prediction_models" / "runs" / str(run_tag_or_dir)).resolve()


def load_projection_run(project_root: str | Path, run_tag_or_dir: str | Path) -> ProjectionModelRun:
    """Load a persisted projection model run for inference."""
    import lightgbm as lgb
    import xgboost as xgb

    run_dir = _resolve_run_dir(project_root, run_tag_or_dir)
    if not run_dir.exists():
        raise FileNotFoundError(f"Projection run directory does not exist: {run_dir}")

    metadata = read_json(run_dir / "metadata.json")
    feature_contract = read_json(run_dir / "feature_contract.json")
    metrics = read_json(run_dir / "metrics.json")
    artifacts_path = run_dir / "artifacts" / "artifacts.json"
    artifacts = read_json(artifacts_path) if artifacts_path.exists() else {}

    model_files = metadata.get("model_files", {})
    stage_a_path = run_dir / model_files.get("stage_a", "stage_a/model.json")
    stage_b_clf_path = run_dir / model_files.get("stage_b_classifier", "stage_b/classifier.txt")
    stage_b_reg_path = run_dir / model_files.get("stage_b_regressor", "stage_b/regressor.txt")
    stage_c_clf_path = run_dir / model_files.get("stage_c_classifier", "stage_c/classifier.txt")
    stage_c_reg_path = run_dir / model_files.get("stage_c_regressor", "stage_c/regressor.txt")

    stage_a_booster = xgb.Booster()
    stage_a_booster.load_model(str(stage_a_path))
    stage_b_clf = lgb.Booster(model_file=str(stage_b_clf_path))
    stage_b_reg = lgb.Booster(model_file=str(stage_b_reg_path))
    stage_c_clf = lgb.Booster(model_file=str(stage_c_clf_path))
    stage_c_reg = lgb.Booster(model_file=str(stage_c_reg_path))

    counts = metadata.get("training_counts", {})
    stage_a = StageAModel(
        booster=stage_a_booster,
        features=list(feature_contract["stage_a_features"]),
        class_order=list(feature_contract["class_order"]),
        train_rows=int(counts.get("stage_a_train_rows", 0) or 0),
        holdout_rows=int(counts.get("stage_a_holdout_rows", 0) or 0),
    )
    stage_b = StageBModel(
        clf=stage_b_clf,
        reg=stage_b_reg,
        features=list(feature_contract["stage_b_features"]),
        train_rows=int(counts.get("stage_b_train_rows", 0) or 0),
        holdout_rows=int(counts.get("stage_b_holdout_rows", 0) or 0),
    )
    stage_c = StageCRecoveryModel(
        clf=stage_c_clf,
        reg=stage_c_reg,
        features=list(feature_contract["stage_c_recovery_features"]),
        fallback_by_class=dict(artifacts.get("recovery_by_class", {})),
        train_rows=int(counts.get("stage_c_train_rows", 0) or 0),
        holdout_rows=int(counts.get("stage_c_holdout_rows", 0) or 0),
        train_positive_rows=int(counts.get("stage_c_positive_train_rows", 0) or 0),
        min_days_since_default=int(feature_contract.get("stage_c_min_days_since_default", 180)),
    )

    return ProjectionModelRun(
        run_dir=run_dir,
        metadata=metadata,
        feature_contract=feature_contract,
        metrics=metrics,
        artifacts=artifacts,
        stage_a=stage_a,
        stage_b=stage_b,
        stage_c=stage_c,
    )
