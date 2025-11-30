from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import max, col, lit, current_timestamp
from pyspark.sql.types import TimestampType
from delta.tables import DeltaTable
import logging

logger = logging.getLogger(__name__)

def get_next_surrogate_key_start(spark, path, key_col):
    if DeltaTable.isDeltaTable(spark, path):
        # Handle case where table exists but is empty
        try:
            max_val = spark.read.format("delta").load(path).agg(max(key_col)).collect()[0][0]
            return (max_val or 0) + 1
        except:
            return 1
    return 1

def upsert_scd_type_2(spark, df_staging, target_path, key_column):
    if DeltaTable.isDeltaTable(spark, target_path):
        delta_table = DeltaTable.forPath(spark, target_path)
        delta_table.alias("target").merge(
            df_staging.alias("source"),
            f"target.{key_column} = source.{key_column} AND target.is_current = true"
        ).whenMatchedUpdate(
            set={
                "is_current": lit(False),
                "end_date": current_timestamp()
            }
        ).execute()   
        df_staging.write.format("delta").mode("append").save(target_path)
    else:
        df_staging.write.format("delta").mode("overwrite").save(target_path)

def upload_to_bigquery(spark: SparkSession, df: DataFrame, table_id: str, temp_bucket: str, labels: dict = None):
    """
    Uploads a PySpark DataFrame to BigQuery.
    Assumes table exists (managed by Terraform) and overwrites data.
    """
    try:
        logger.info(f"Uploading data to BigQuery table: {table_id}")
        
        writer = (
            df.write.format("bigquery")
            .option("table", table_id)
            .option("temporaryGcsBucket", temp_bucket)
        )
        
        if labels:
            labels_str = ",".join([f"{k}:{v}" for k, v in labels.items()])
            writer = writer.option("labels", labels_str)

        # mode("overwrite") acts as WRITE_TRUNCATE in BigQuery, preserving schema/clustering
        writer.mode("overwrite").save()
        
        logger.info(f"Successfully uploaded data to {table_id}")

    except Exception as e:
        logger.error(f"Failed to upload to BigQuery: {e}")
        raise
