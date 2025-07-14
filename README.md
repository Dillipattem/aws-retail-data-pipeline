# 🛒 Retail Data Engineering Pipeline on AWS

This project showcases a complete data engineering pipeline using AWS services to transform and analyze retail data stored in CSV files. The goal was to convert raw data into queryable insights using Glue, Athena, and S3.

---

## 🚀 Tech Stack

- **Amazon S3** – Raw and prepared zone storage
- **AWS Glue (Python Shell)** – ETL transformations and format conversion
- **AWS Glue Crawler** – Automatic schema generation in Glue Data Catalog
- **Amazon Athena** – SQL querying of Parquet files

---

## 🔄 Pipeline Steps

1. **CSV Upload to S3**  
   Raw retail CSV files uploaded to:  
   `s3://gluetasks/raw/`

2. **ETL with AWS Glue**  
   - CSV → Parquet conversion
   - Transformations like:
     - City/state formatting
     - Flags and derived columns (e.g., total_price, high_value_flag)

3. **S3 Output (Prepared)**  
   Cleaned Parquet files written to:  
   `s3://gluetasks/prepared/`

4. **Glue Crawler**  
   Auto-generates Glue tables in `retail_prepared_db`.

5. **Querying via Athena**  
   SQL queries on Parquet datasets for insights.

---

## 📂 Sample Queries

```sql
-- Top Products by Revenue
SELECT p.product_name, SUM(oi.total_price) AS revenue
FROM order_items oi
JOIN products p ON p.product_id = oi.product_id
GROUP BY p.product_name
ORDER BY revenue DESC;
