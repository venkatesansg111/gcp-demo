import argparse
from pathlib import Path

import yaml
from google.cloud import storage


REQUIRED_KEYS = {
    "composer_bucket",
    "input_bucket",
    "dataproc_job_bucket",
}


def load_config(config_path: Path) -> dict:
    with config_path.open("r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    missing = REQUIRED_KEYS.difference(config.keys())
    if missing:
        raise ValueError(f"Missing config keys: {sorted(missing)}")
    return config


def upload_dir(client: storage.Client, local_dir: Path, bucket_name: str, prefix: str) -> None:
    bucket = client.bucket(bucket_name)
    for file_path in local_dir.rglob("*"):
        if file_path.is_file():
            blob_name = f"{prefix}/{file_path.relative_to(local_dir).as_posix()}"
            bucket.blob(blob_name).upload_from_filename(str(file_path))
            print(f"Uploaded {file_path} -> gs://{bucket_name}/{blob_name}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Upload DAGs, jobs, and data files.")
    parser.add_argument("--config", required=True, help="Path to config YAML file.")
    parser.add_argument(
        "--repo-root",
        default=str(Path(__file__).resolve().parents[1]),
        help="Repository root path.",
    )
    args = parser.parse_args()

    repo_root = Path(args.repo_root)
    config = load_config(Path(args.config))
    client = storage.Client(project=config.get("project_id"))

    upload_dir(client, repo_root / "composer" / "dags", config["composer_bucket"], "dags")
    upload_dir(client, repo_root / "dataproc" / "jobs", config["dataproc_job_bucket"], "jobs")

    sales_file = repo_root / "data" / "sales.csv"
    data_blob = client.bucket(config["input_bucket"]).blob("sales.csv")
    data_blob.upload_from_filename(str(sales_file))
    print(f"Uploaded {sales_file} -> gs://{config['input_bucket']}/sales.csv")


if __name__ == "__main__":
    main()
