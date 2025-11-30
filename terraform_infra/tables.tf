resource "google_bigquery_table" "dim_customer" {
  dataset_id          = google_bigquery_dataset.analytics_datawarehouse.dataset_id
  table_id            = "dim_customer"
  clustering          = ["country_name"]
  deletion_protection = false

  schema = <<EOF
[
  {"name": "customer_key", "type": "INTEGER"},
  {"name": "customer_id", "type": "INTEGER"},
  {"name": "country_name", "type": "STRING"},
  {"name": "is_current", "type": "BOOLEAN"},
  {"name": "start_date", "type": "TIMESTAMP"},
  {"name": "end_date", "type": "TIMESTAMP"}
]
EOF
}

resource "google_bigquery_table" "dim_country" {
  dataset_id          = google_bigquery_dataset.analytics_datawarehouse.dataset_id
  table_id            = "dim_country"
  deletion_protection = false

  schema = <<EOF
[
  {"name": "country_key", "type": "INTEGER"},
  {"name": "country_name", "type": "STRING"},
  {"name": "is_current", "type": "BOOLEAN"},
  {"name": "start_date", "type": "TIMESTAMP"},
  {"name": "end_date", "type": "TIMESTAMP"}
]
EOF
}

resource "google_bigquery_table" "dim_date" {
  dataset_id          = google_bigquery_dataset.analytics_datawarehouse.dataset_id
  table_id            = "dim_date"
  clustering          = ["year", "month"]
  deletion_protection = false

  schema = <<EOF
[
  {"name": "date_key", "type": "INTEGER"},
  {"name": "date", "type": "DATE"},
  {"name": "year", "type": "INTEGER"},
  {"name": "month", "type": "INTEGER"},
  {"name": "day", "type": "INTEGER"},
  {"name": "day_of_week", "type": "INTEGER"},
  {"name": "week_of_year", "type": "INTEGER"},
  {"name": "quarter", "type": "INTEGER"},
  {"name": "is_current", "type": "BOOLEAN"},
  {"name": "start_date", "type": "TIMESTAMP"},
  {"name": "end_date", "type": "TIMESTAMP"}
]
EOF
}

resource "google_bigquery_table" "dim_product" {
  dataset_id          = google_bigquery_dataset.analytics_datawarehouse.dataset_id
  table_id            = "dim_product"
  clustering          = ["stock_code"]
  deletion_protection = false

  schema = <<EOF
[
  {"name": "product_key", "type": "INTEGER"},
  {"name": "stock_code", "type": "STRING"},
  {"name": "description", "type": "STRING"},
  {"name": "is_current", "type": "BOOLEAN"},
  {"name": "start_date", "type": "TIMESTAMP"},
  {"name": "end_date", "type": "TIMESTAMP"}
]
EOF
}

resource "google_bigquery_table" "fact_transaction" {
  dataset_id          = google_bigquery_dataset.analytics_datawarehouse.dataset_id
  table_id            = "fact_transaction"
  clustering          = ["customer_key", "product_key", "country_key"]
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "invoice_date_date"
  }

  schema = <<EOF
[
  {"name": "transaction_key", "type": "INTEGER"},
  {"name": "customer_key", "type": "INTEGER"},
  {"name": "product_key", "type": "INTEGER"},
  {"name": "country_key", "type": "INTEGER"},
  {"name": "date_key", "type": "INTEGER"},
  {"name": "invoice", "type": "STRING"},
  {"name": "quantity", "type": "INTEGER"},
  {"name": "price", "type": "FLOAT"},
  {"name": "revenue", "type": "FLOAT"},
  {"name": "invoice_date_date", "type": "DATE"}
]
EOF
}

# Gold Tables

resource "google_bigquery_table" "revenue_monthly" {
  dataset_id          = google_bigquery_dataset.analytics_datamart.dataset_id
  table_id            = "revenue_monthly"
  clustering          = ["year", "month"]
  deletion_protection = false

  schema = <<EOF
[
  {"name": "year", "type": "INTEGER"},
  {"name": "month", "type": "INTEGER"},
  {"name": "total_revenue", "type": "NUMERIC"}
]
EOF
}

resource "google_bigquery_table" "product_revenue_performance" {
  dataset_id          = google_bigquery_dataset.analytics_datamart.dataset_id
  table_id            = "product_revenue_performance"
  clustering          = ["stock_code"]
  deletion_protection = false

  schema = <<EOF
[
  {"name": "stock_code", "type": "STRING"},
  {"name": "description", "type": "STRING"},
  {"name": "total_revenue", "type": "NUMERIC"}
]
EOF
}

resource "google_bigquery_table" "revenue_country" {
  dataset_id          = google_bigquery_dataset.analytics_datamart.dataset_id
  table_id            = "revenue_country"
  clustering          = ["country_name"]
  deletion_protection = false

  schema = <<EOF
[
  {"name": "country_name", "type": "STRING"},
  {"name": "total_revenue", "type": "NUMERIC"}
]
EOF
}

resource "google_bigquery_table" "rfm_analysis" {
  dataset_id          = google_bigquery_dataset.analytics_datamart.dataset_id
  table_id            = "rfm_analysis"
  clustering          = ["Recency", "Frequency"]
  deletion_protection = false

  schema = <<EOF
[
  {"name": "customer_id", "type": "INTEGER"},
  {"name": "Recency", "type": "INTEGER"},
  {"name": "Frequency", "type": "INTEGER"},
  {"name": "Monetary", "type": "NUMERIC"}
]
EOF
}
