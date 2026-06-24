import argparse
from pathlib import Path

import yaml
from google.cloud import storage


REQUIRED_KEYS = {
    "composer_bucket",
}


def load_config(config_path: Path) -> dict:
    with config_path.open("r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    missing = REQUIRED_KEYS.difference(config.keys())
    if missing:
        raise ValueError(f"Missing config keys: {sorted(missing)}")
    return config


def main() -> None:
    parser = argparse.ArgumentParser(description="Upload DAGs and runtime config to Composer bucket.")
    parser.add_argument("--config", required=True, help="Path to environment config YAML.")
    parser.add_argument(
        "--repo-root",
        default=str(Path(__file__).resolve().parents[1]),
        help="Repository root path.",
    )
    args = parser.parse_args()

    repo_root = Path(args.repo_root)
    config_path = Path(args.config)
    config = load_config(config_path)

    client = storage.Client(project=config.get("project_id"))
    bucket = client.bucket(config["composer_bucket"])

    dags_dir = repo_root / "composer" / "dags"
    for file_path in dags_dir.rglob("*.py"):
        blob_name = f"dags/{file_path.relative_to(dags_dir).as_posix()}"
        bucket.blob(blob_name).upload_from_filename(str(file_path))
        print(f"Uploaded {file_path} -> gs://{config['composer_bucket']}/{blob_name}")

    runtime_blob = bucket.blob("dags/config/runtime_config.yaml")
    runtime_blob.upload_from_filename(str(config_path))
    print(
        f"Uploaded {config_path} -> gs://{config['composer_bucket']}/dags/config/runtime_config.yaml"
    )


if __name__ == "__main__":
    main()
