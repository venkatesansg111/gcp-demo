DECLARE project_id STRING DEFAULT @project_id;
DECLARE dataset_id STRING DEFAULT @dataset_id;

EXECUTE IMMEDIATE FORMAT(
  "CREATE SCHEMA IF NOT EXISTS `%s.%s`",
  project_id,
  dataset_id
);
