import argparse
from pathlib import Path

import yaml
from google.cloud import bigquery


REQUIRED_KEYS = {
    "project_id",
    "region",
    "bq_dataset",
    "bq_table",
}


SCHEMA = [
    bigquery.SchemaField("order_id", "STRING"),
    bigquery.SchemaField("customer_name", "STRING"),
    bigquery.SchemaField("country", "STRING"),
    bigquery.SchemaField("product", "STRING"),
    bigquery.SchemaField("quantity", "INTEGER"),
    bigquery.SchemaField("price", "FLOAT"),
    bigquery.SchemaField("total_amount", "FLOAT"),
]


def load_config(config_path: Path) -> dict:
    with config_path.open("r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    missing = REQUIRED_KEYS.difference(config.keys())
    if missing:
        raise ValueError(f"Missing config keys: {sorted(missing)}")
    return config


def main() -> None:
    parser = argparse.ArgumentParser(description="Create BigQuery dataset and table if they do not exist.")
    parser.add_argument("--config", required=True, help="Path to environment config YAML.")
    args = parser.parse_args()

    config = load_config(Path(args.config))
    client = bigquery.Client(project=config["project_id"])

    dataset_id = f"{config['project_id']}.{config['bq_dataset']}"
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = config["region"]
    client.create_dataset(dataset, exists_ok=True)
    print(f"Dataset ready: {dataset_id}")

    table_id = f"{dataset_id}.{config['bq_table']}"
    table = bigquery.Table(table_id, schema=SCHEMA)
    client.create_table(table, exists_ok=True)
    print(f"Table ready: {table_id}")


if __name__ == "__main__":
    main()
