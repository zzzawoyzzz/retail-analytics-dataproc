from argparse import ArgumentParser
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    FloatType,
    IntegerType,
    TimestampType,
)
from pyspark.sql.functions import (
    col,
    current_timestamp,
    lit,
    to_date,
    year,
    month,
    dayofmonth,
    dayofweek,
    weekofyear,
    quarter,
    monotonically_increasing_id,
    when,
    datediff,
    max,
    countDistinct,
    sum,
    row_number,
    broadcast,
    round,
    ntile,
    date_add,
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import logging
import yaml

from utils import get_next_surrogate_key_start, upsert_scd_type_2, upload_to_bigquery

logger = logging.getLogger(__name__)


def validate_path(input_path: str, output_path: str):

    if not input_path:
        logger.warning("input_path argument is empty.")
        raise ValueError(
            "input_path argument is empty. Provide a valid GCS path, e.g. gs://bucket/folder/file.csv"
        )
    output_path = (output_path or "").strip().rstrip("/")
    if not output_path:
        logger.warning("output_path argument is empty")
        raise ValueError(
            "output_path argument is empty. Provide a valid GCS folder, e.g. gs://bucket/delta/customer_dim"
        )
    return


def create_spark_session():
    try:
        spark = (
            SparkSession.builder.appName("delta_ingestion_job")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .getOrCreate()
        )
        return spark
    except Exception as e:
        logger.error("error create spark session")
        logger.error(f"Error occurred: {str(e)}")
        raise

    return spark

def load_config(spark, config_path):
    logger.info(f"Loading config from {config_path}")
    config_content = "\n".join([row.value for row in spark.read.text(config_path).collect()])
    return yaml.safe_load(config_content)


def read_source_to_bronze (spark: SparkSession, input_path: str, warehouse_path: str, config : dict) :
    
    logger.info(f"start run read df from {input_path}")
    output_bronze_path = f"{warehouse_path}/{config['paths']['layers']['bronze']}"
    dataset_bronze = config['datasets']['bronze']
    project_id = config['project_id']
    
    table_bronze = config['tables']['bronze']
    schema = StructType(
        [
            StructField("Invoice", StringType(), nullable=True),
            StructField("StockCode", StringType(), nullable=True),
            StructField("Description", StringType(), nullable=True),
            StructField("Quantity", IntegerType(), nullable=True),
            StructField("InvoiceDate", TimestampType(), nullable=True),
            StructField("Price", FloatType(), nullable=True),
            StructField("Customer_ID", FloatType(), nullable=True),
            StructField("Country", StringType(), nullable=True),
        ]
    )
    try:
        df = spark.read.csv(input_path, header=True, schema=schema)

    except Exception as e:
        logger.error(f"Error occurred: {str(e)}")
        raise
    
    logger.info("add metadata to df")
    df = df.withColumn("ingesttion_time", current_timestamp()).withColumn(
        "source_system", lit("csv")
    )
    df = df.withColumn("InvoiceDate_Date", to_date(col("InvoiceDate")))
    df = df.select(
        col("Invoice").alias("invoice"),
        col("StockCode").alias("stock_code"),
        col("Description").alias("description"),
        col("Quantity").alias("quantity"),
        col("InvoiceDate").alias("invoice_date"),
        col("Price").alias("price"),
        col("Customer_ID").alias("customer_id"),
        col("Country").alias("country"),
        col("ingesttion_time").alias("ingesttion_time"),
        col("source_system").alias("source_system"),
        col("InvoiceDate_Date").alias("invoice_date_date"),
    )
    
    logger.info(f"writing file to {output_bronze_path}")
    df.write.format("delta").mode("overwrite").partitionBy("invoice_date_date").save(output_bronze_path)
    
    
    labels = config.get('table_labels', {}).get('bronze', {})
    upload_to_bigquery(spark, df, f"{project_id}.{dataset_bronze}.{table_bronze}", config['buckets']['temp'], labels=labels)
    logger.info(f"end spark success, wrote data to {output_bronze_path}")
    return output_bronze_path




def cleanse_and_load_to_silver(spark: SparkSession, bronze_path: str, warehouse_path: str,config:dict):
    
    df = spark.read.format("delta").load(bronze_path).cache()
    logger.info("start cleanse and load to silver")
    
    
    # 1. Customer Dimension
    path_dim_customer = f"{warehouse_path}/{config['paths']['silver_tables']['dim_customer']}"
    start_cust_key = get_next_surrogate_key_start(spark, path_dim_customer, "customer_key")
    
    df_dim_customer = (
        df.select(col("customer_id"), col("country"), col("ingesttion_time"))
        .withColumn("rank", row_number().over(Window.partitionBy("customer_id").orderBy(col("ingesttion_time").desc())))
        .where(col("customer_id").isNotNull() & (col("rank") == 1))
        .withColumn("customer_id", col("customer_id").cast("int"))
        .withColumnRenamed("country", "country_name")
        .drop("rank", "ingesttion_time")
    )

    df_dim_customer = df_dim_customer.withColumn(
        "customer_key", (row_number().over(Window.orderBy("customer_id")) + lit(start_cust_key) - 1).cast("int")
    ).select(col("customer_key"), col("customer_id"), col("country_name"))


    # 2. Product Dimension
    path_dim_product = f"{warehouse_path}/{config['paths']['silver_tables']['dim_product']}"
    start_prod_key = get_next_surrogate_key_start(spark, path_dim_product, "product_key")

    df_dim_product = (
        df.select(
            col("stock_code"),
            col("ingesttion_time"),
            when(col("description").isNull(), lit("Unknown"))
            .otherwise(col("description"))
            .alias("description")
            
        )
        .withColumn("rank", row_number().over(Window.partitionBy("stock_code").orderBy(col("ingesttion_time").desc())))
        .where(col("stock_code").isNotNull() & (col("rank") == 1))
        .drop("rank", "ingesttion_time")
    )
    
    df_dim_product = df_dim_product.withColumn(
        "product_key", (row_number().over(Window.orderBy("stock_code")) + lit(start_prod_key) - 1).cast("int")
    ).select(
        col("product_key"), 
        col("stock_code"), 
        col("description"),
        )

    # 3. Country Dimension
    path_dim_country = f"{warehouse_path}/{config['paths']['silver_tables']['dim_country']}"
    start_country_key = get_next_surrogate_key_start(spark, path_dim_country, "country_key")

    df_dim_country = (
        df.select(col("country"), col("ingesttion_time"))
        .withColumn("rank", row_number().over(Window.partitionBy("country").orderBy(col("ingesttion_time").desc())))
        .where(col("country").isNotNull() & (col("rank") == 1))
        .drop("rank", "ingesttion_time")
    )

    df_dim_country = df_dim_country.withColumn(
        "country_key", (row_number().over(Window.orderBy("country")) + lit(start_country_key) - 1).cast("int")
    ).select(col("country_key"), col("country").alias("country_name"))

    # 4. Date Dimension
    path_dim_date = f"{warehouse_path}/{config['paths']['silver_tables']['dim_date']}"
    start_date_key = get_next_surrogate_key_start(spark, path_dim_date, "date_key")

    df_dim_date = (
        df.select(col("invoice_date").alias("full_date"), col("ingesttion_time"))
        .withColumn("date", to_date(col("full_date")))
        .withColumn("year", year(col("full_date")))
        .withColumn("month", month(col("full_date")))
        .withColumn("day", dayofmonth(col("full_date")))
        .withColumn("day_of_week", dayofweek(col("full_date")))
        .withColumn("week_of_year", weekofyear(col("full_date")))
        .withColumn("quarter", quarter(col("full_date")))
        .withColumn("rank", row_number().over(Window.partitionBy("date").orderBy(col("ingesttion_time").desc())))
        .where(col("rank") == 1)
        .drop("rank", "ingesttion_time")
    )

    df_dim_date = df_dim_date.withColumn(
        "date_key", (row_number().over(Window.orderBy("date")) + lit(start_date_key) - 1).cast("int")
    ).select(
        col("date_key"),
        col("date"),
        col("year"),
        col("month"),
        col("day"),
        col("day_of_week"),
        col("week_of_year"),
        col("quarter"),
    )

    # 5. Fact Table
    logger.debug("start join df_fact with dimension table at silver")
    df_fact = df.withColumn("date", to_date(col("invoice_date"))).where(~col("invoice").startswith("C")) #filter out cancelled transaction
    df_fact = df_fact.alias("fact").join(broadcast(df_dim_customer.alias("cust")), col("fact.customer_id") == col("cust.customer_id"), "left") \
                     .join(broadcast(df_dim_product.alias("prod")), col("fact.stock_code") == col("prod.stock_code"), "left") \
                     .join(broadcast(df_dim_country.alias("cntry")), col("fact.country") == col("cntry.country_name"), "left") \
                     .join(broadcast(df_dim_date.alias("dt")), col("fact.date") == col("dt.date"), "left")
    logger.debug("end join df_fact with dimension table at silver")
    df_transaction = (
        df_fact.withColumn("transaction_key", row_number().over(Window.orderBy("invoice")))
        .withColumn("revenue", col("fact.quantity") * col("fact.price"))
        .select(
            col("transaction_key"),
            col("cust.customer_key"),
            col("prod.product_key"),
            col("cntry.country_key"),
            col("dt.date_key"),
            col("fact.invoice").alias("invoice"),
            col("fact.quantity").alias("quantity"),
            col("fact.price").alias("price"),
            col("revenue"),
            col("fact.date").alias("invoice_date_date")
        )
    )

    df.unpersist()

    # Get BigQuery config upfront
    project_id = config['project_id']
    dataset_datawarehouse = config['datasets']['datawarehouse']
    temp_bucket = config['buckets']['temp']
    
    logger.info("start write silver and upload to BigQuery")
    
    # 1. Customer Dimension
    df_dim_customer_staging = df_dim_customer.withColumn("is_current", lit(True)) \
                                             .withColumn("start_date", current_timestamp()) \
                                             .withColumn("end_date", lit(None).cast(TimestampType()))
    upsert_scd_type_2(spark, df_dim_customer_staging, path_dim_customer, "customer_id")
    df_current_customer = spark.read.format("delta").load(path_dim_customer)
    labels = config.get('table_labels', {}).get('dim_customer', {})
    upload_to_bigquery(spark, df_current_customer, f"{project_id}.{dataset_datawarehouse}.dim_customer", temp_bucket, labels=labels)
    logger.info("Customer dimension uploaded to BigQuery")

    # 2. Country Dimension
    df_dim_country_staging = df_dim_country.withColumn("is_current", lit(True)) \
                                           .withColumn("start_date", current_timestamp()) \
                                           .withColumn("end_date", lit(None).cast(TimestampType()))
    upsert_scd_type_2(spark, df_dim_country_staging, path_dim_country, "country_name")
    df_current_country = spark.read.format("delta").load(path_dim_country)
    labels = config.get('table_labels', {}).get('dim_country', {})
    upload_to_bigquery(spark, df_current_country, f"{project_id}.{dataset_datawarehouse}.dim_country", temp_bucket, labels=labels)
    logger.info("Country dimension uploaded to BigQuery")

    # 3. Date Dimension (Usually static, but applying same logic for consistency if requested)
    df_dim_date_staging = df_dim_date.withColumn("is_current", lit(True)) \
                                     .withColumn("start_date", current_timestamp()) \
                                     .withColumn("end_date", lit(None).cast(TimestampType()))
    upsert_scd_type_2(spark, df_dim_date_staging, path_dim_date, "date_key") # date_key is surrogate, ideally use 'date'
    df_current_date = spark.read.format("delta").load(path_dim_date)
    labels = config.get('table_labels', {}).get('dim_date', {})
    upload_to_bigquery(spark, df_current_date, f"{project_id}.{dataset_datawarehouse}.dim_date", temp_bucket, labels=labels)
    logger.info("Date dimension uploaded to BigQuery")

    # 4. Product Dimension
    df_dim_product_staging = df_dim_product.withColumn("is_current", lit(True)) \
                                           .withColumn("start_date", current_timestamp()) \
                                           .withColumn("end_date", lit(None).cast(TimestampType()))
    upsert_scd_type_2(spark, df_dim_product_staging, path_dim_product, "stock_code")
    df_current_product = spark.read.format("delta").load(path_dim_product)
    labels = config.get('table_labels', {}).get('dim_product', {})
    upload_to_bigquery(spark, df_current_product, f"{project_id}.{dataset_datawarehouse}.dim_product", temp_bucket, labels=labels)
    logger.info("Product dimension uploaded to BigQuery")

    # 5. Fact Table (Always Append or Overwrite, usually partitioned)
    path_fact_transaction = f"{warehouse_path}/{config['paths']['silver_tables']['fact_transaction']}"
    df_transaction.write.format("delta").mode("overwrite").partitionBy("invoice_date_date").save(path_fact_transaction)
    df_current_fact = spark.read.format("delta").load(path_fact_transaction)
    labels = config.get('table_labels', {}).get('fact_transaction', {})
    upload_to_bigquery(spark, df_current_fact, f"{project_id}.{dataset_datawarehouse}.fact_transaction", temp_bucket, labels=labels)
    logger.info("Fact transaction uploaded to BigQuery")
    
    logger.info("All silver tables written to Delta Lake and uploaded to BigQuery")
    
    return path_fact_transaction, path_dim_customer, path_dim_country, path_dim_date, path_dim_product


def aggregate_and_load_to_gold(spark: SparkSession, warehouse_path: str, path_fact: str, path_cust: str, path_country: str, path_date: str, path_prod: str ,config:dict):
    df_fact = spark.read.format("delta").load(path_fact)
    df_dim_customer = spark.read.format("delta").load(path_cust)
    df_dim_country = spark.read.format("delta").load(path_country)
    df_dim_date = spark.read.format("delta").load(path_date)
    df_dim_product = spark.read.format("delta").load(path_prod)
    
    logger.info("1. transform Calculating revenue monthly")
    # 1.calculate revenue monthly
    df_dim_date = df_dim_date.where(col("is_current") == True)
    df_revenue_monthly = (df_fact.join(df_dim_date, df_fact["date_key"] == df_dim_date["date_key"], "left")
                                .groupBy("year","month")
                                .agg(sum("revenue").alias("total_revenue"))
                                .orderBy("year","month",ascending=False)
                                .select(    
                                    col("year"),
                                    col("month"),
                                    round(col("total_revenue"),2).cast("decimal(18,2)").alias("total_revenue")
                                ))
    # path_revenue_monthly = gs://02-processed_data_dataproc/delta/online_retail_II/gold/revenue_monthly
    path_revenue_monthly = f"{warehouse_path}/{config['paths']['gold_tables']['revenue_monthly']}"
    logger.debug(f"path_revenue_monthly : {path_revenue_monthly}")
    df_revenue_monthly.write.format("delta").mode("overwrite").save(path_revenue_monthly)
    logger.info("2. transform Calculating revenue monthly")
    
    # 2.calculate product revenue performance
    df_dim_product = df_dim_product.where(col("is_current") == True)
    df_product_revenue_performance = (df_fact.alias("fact")
                                .join(df_dim_product.alias("dim_product"), df_fact["product_key"] == df_dim_product["product_key"], "left")
                                .groupBy("stock_code","description")
                                .agg(sum("revenue").alias("total_revenue"))
                                .orderBy("total_revenue", ascending=False)
                                .limit(10)
                                .select(
                                    col("stock_code"),
                                    col("description"),
                                    round(col("total_revenue"), 2).cast("decimal(18,2)").alias("total_revenue")
                                ))
    path_product_revenue_performance = f"{warehouse_path}/{config['paths']['gold_tables']['product_revenue_performance']}"
    logger.debug(f"path_product_revenue_performance : {path_product_revenue_performance}")
    df_product_revenue_performance.write.format("delta").mode("overwrite").save(path_product_revenue_performance)
    logger.info("3. transform Calculating revenue country")
    
    # 3.calculate revenue per country
    df_dim_country = df_dim_country.where(col("is_current") == True)
    df_revenue_country = (df_fact.alias("fact")
                                .join(df_dim_country.alias("dim_country"), df_fact["country_key"] == df_dim_country["country_key"], "left")
                                .groupBy("country_name")
                                .agg(sum("revenue").alias("total_revenue"))
                                .orderBy("total_revenue", ascending=False)
                                .select(    
                                    col("country_name"),
                                    round(col("total_revenue"), 2).cast("decimal(18,2)").alias("total_revenue"))
                                    )
    path_revenue_country = f"{warehouse_path}/{config['paths']['gold_tables']['revenue_country']}"
    df_revenue_country.write.format("delta").mode("overwrite").save(path_revenue_country)

    # 4. RFM Analysis

    logger.info("4.transform Joining fact with date and customer")
    df_rfm_base = df_fact.alias("f") \
        .join(df_dim_date.alias("d"), "date_key", "left") \
        .join(df_dim_customer.alias("c"), "customer_key", "left")
    

    max_date_row = df_rfm_base.agg(max("d.date")).collect()[0]
    max_date_value = max_date_row[0]
    # Add 1 day to max date to ensure all customers have Recency >= 1
    max_date = date_add(lit(max_date_value), 1)
    # Calculate RFM Metrics
    df_rfm = (
        df_rfm_base.groupBy("c.customer_id")
        .agg(
            datediff(lit(max_date), max("d.date")).alias("Recency"),
            countDistinct("f.invoice").alias("Frequency"),
            sum("f.revenue").alias("Monetary")
        )
    )

    # Calculate RFM Scores (1-5, where 5 is best)
    # R: Lower Recency (days) = Better = Higher Score
    # F: Higher Frequency (orders) = Better = Higher Score  
    # M: Higher Monetary (spend) = Better = Higher Score
    df_rfm = df_rfm.withColumn("R", ntile(5).over(Window.orderBy(col("Recency").asc()))) \
                   .withColumn("F", ntile(5).over(Window.orderBy(col("Frequency").asc()))) \
                   .withColumn("M", ntile(5).over(Window.orderBy(col("Monetary").asc())))
    
    # แบ่งลูกค้าออกเป็น 10 กลุ่มตาม Criteria ของ RFM analysis อันได้แก่ 
    df_rfm = df_rfm.withColumn("Segment", 
        # Champions: เพิ่งซื้อไปไม่นานมานี้ ซื้อบ่อย และ ใช้จ่ายเยอะ 
        when((col("R") >= 4) & (col("F") >= 4) & (col("M") >= 4), "Champions")
        
        # Loyal Customers: ซื้อบ่อย และ ใช้จ่ายเยอะ
        .when((col("R") >= 3) & (col("F") >= 4), "Loyal customers")
        
        # Potential Loyalists: เพิ่งซื้อไปไม่นาน ใช้จ่ายปานกลางถึงสูง และซื้อมากกว่า 1 ครั้ง 
        .when((col("R") >= 3) & (col("F") >= 2) & (col("F") <= 3) & (col("M") >= 2), "Potential Loyalists")
        
        # New Customers: เพิ่งซื้อไปไม่นาน และซื้อสินค้าไม่บ่อย
        .when((col("R") >= 4) & (col("F") <= 2), "New customers")
        
        # Promising: เพิ่งซื้อไปไม่นาน และใช้จ่ายน้อย
        .when((col("R") >= 3) & (col("F") <= 2) & (col("M") <= 2), "Promising")
        
        # Need Attention: เหนือกว่าค่าเฉลี่ยทั้งหมด 
        .when((col("R") >= 2) & (col("R") <= 3) & (col("F") >= 2) & (col("F") <= 3) & (col("M") >= 2), "Need Attention")
        
        # Long time big buy: ใช้จ่ายเยอะ นานๆ ครั้ง
        .when((col("R") <= 2) & (col("M") >= 4), "Long time big buy")
        
        # Small basket size: ใช้จ่ายไม่เยอะ แต่ซื้อบ่อย และ เพิ่งซื้อไปไม่นาน
        .when((col("R") >= 3) & (col("M") <= 2), "Small basket size")
        
        # At risk: ใช้จ่ายเยอะ เมื่อนานมากๆ แล้ว
        .when((col("R") <= 2) & (col("F") <= 2), "At risk")
        
        # Hibernating: ใช้จ่ายน้อย ซื้อไม่บ่อย และ ซื้อครั้งล่าสุดเมื่อนานมาแล้ว 
        .when((col("R") == 1) & (col("F") <= 2), "Hibernating")
        
        # Others: อื่นๆ
        .otherwise("Others")
    ).select(
        col("customer_id"),
        col("Recency"),
        col("Frequency"),
        round(col("Monetary"), 2).cast("decimal(18,2)").alias("Monetary"),
        col("R").alias("R_Score"),
        col("F").alias("F_Score"),
        col("M").alias("M_Score"),
        col("Segment")
    )
    
    path_rfm = f"{warehouse_path}/{config['paths']['gold_tables']['rfm_analysis']}"
    logger.debug(f"path_rfm : {path_rfm}")
    df_rfm.write.format("delta").mode("overwrite").save(path_rfm)
    

    # --- Load Gold Tables to BigQuery ---
    logger.info("Uploading gold tables to BigQuery")
    project_id = config['project_id']
    dataset_datamart = config['datasets']['datamart']
    temp_bucket = config['buckets']['temp']
    
    gold_tables = {
        config['tables']['revenue_monthly']: path_revenue_monthly,
        config['tables']['product_revenue_performance']: path_product_revenue_performance,
        config['tables']['revenue_country']: path_revenue_country,
        config['tables']['rfm_analysis']: path_rfm
    }

    for table_name, path in gold_tables.items():
        df_gold = spark.read.format("delta").load(path)
        
        # Get labels for this table
        labels = config.get('table_labels', {}).get(table_name, {})
        
        upload_to_bigquery(spark, df_gold, f"{project_id}.{dataset_datamart}.{table_name}", temp_bucket, labels=labels)
        logger.info(f"Uploaded {table_name} to BigQuery")
    
    logger.info("All gold tables written to Delta Lake and uploaded to BigQuery")


def main(input_path: str, warehouse_path: str, project_id: str, config_file: str):
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%d/%m/%y %H:%M:%S')
    logger.setLevel(logging.INFO)

    validate_path(input_path, warehouse_path)
    spark = create_spark_session()
    config = load_config(spark, config_file)
    # Bronze Layer
    bronze_path = read_source_to_bronze(spark, input_path, warehouse_path,config)
    # Silver Layer
    path_fact_transaction, path_dim_customer, path_dim_country, path_dim_date, path_dim_product = cleanse_and_load_to_silver(spark, bronze_path, warehouse_path,config)

    # Gold Layer
    aggregate_and_load_to_gold(spark, warehouse_path, path_fact_transaction, path_dim_customer, path_dim_country, path_dim_date, path_dim_product,config)

    spark.stop()

if __name__ == "__main__":
    parser = ArgumentParser(description="PySpark Job for Delta Lake ETL")
    parser.add_argument("-i", "--input", required=True, help="Input GCS path for raw data")
    parser.add_argument("-o", "--output", required=True, help="Output GCS path for processed data")
    parser.add_argument("--project_id", required=True, help="GCP Project ID")
    parser.add_argument("--config_file", required=True, help="GCS path to config.yaml")
    
    args = parser.parse_args()
    
    main(args.input, args.output, args.project_id, args.config_file)
