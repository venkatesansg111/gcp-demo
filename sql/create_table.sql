DECLARE project_id STRING DEFAULT @project_id;
DECLARE dataset_id STRING DEFAULT @dataset_id;
DECLARE table_id STRING DEFAULT @table_id;

EXECUTE IMMEDIATE FORMAT(
  """
  CREATE TABLE IF NOT EXISTS `%s.%s.%s` (
    order_id STRING,
    customer_name STRING,
    country STRING,
    product STRING,
    quantity INT64,
    price FLOAT64,
    total_amount FLOAT64
  )
  """,
  project_id,
  dataset_id,
  table_id
);
