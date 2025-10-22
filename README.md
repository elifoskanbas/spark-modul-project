# PySpark Data Processing and Sales Analysis Project 

This project processes and analyzes large-scale sales data using **PySpark** and **Spark SQL**.  
It was prepared for the **Huawei-Advanced Data Processing module** in the **Artificial Intelligence Specialization Program** at the National Technology Academy.

---

## Project Objectives

- Compare sales performance across different regions by processing sales data.

**Task 1:** Analyze the sales of "Jacket" products by region.  
**Task 2:** Calculate the maximum turnover of seller regions.

---

## Project Steps

### 1. Environment Setup and Data Loading

The project was implemented in a **Google Colab** environment with PySpark installed.  
Datasets were read from Google Drive.

```python
from pyspark.sql import SparkSession
from google.colab import drive

# Start Spark Session
spark = SparkSession.builder.appName("PySparkGoogleColab").getOrCreate()

# Mount Google Drive
drive.mount('/content/drive')

file_path = "/content/drive/My Drive/spark_data/"

# Load Parquet files
customer_df = spark.read.parquet(file_path + "customer.parquet")
date_df = spark.read.parquet(file_path + "date.parquet")
product_df = spark.read.parquet(file_path + "product.parquet")
purchase_df = spark.read.parquet(file_path + "purchase.parquet")
region_df = spark.read.parquet(file_path + "region.parquet")
retailer_df = spark.read.parquet(file_path + "retailer.parquet")
sale_df = spark.read.parquet(file_path + "sale.parquet")
supplier_df = spark.read.parquet(file_path + "supplier.parquet")

# Preview DataFrames
product_df.show(3)
region_df.show(3)
sale_df.show(3)
retailer_df.show(3)



### 2. Assignment 1: Regional Analysis of Jacket Sales
Calculate the total quantity of "Jacket" products sold per region.
#### 2.1 PySpark Solution 
from pyspark.sql.functions import sum

# Filter Jacket products
jacket_df = product_df.filter(product_df.product_type == "Jacket")

# Join DataFrames
jacket_sales_df = sale_df.join(jacket_df, on="product_id", how="inner")
jacket_sales_retailer_df = jacket_sales_df.join(retailer_df, on="retailer_id", how="inner")
jacket_sales_region_df = jacket_sales_retailer_df.join(region_df, on="city_id", how="inner")

# Calculate total sales per region
jacket_sales_summary = jacket_sales_region_df.groupBy("region_name").agg(sum("quantity").alias("total_sales"))
jacket_sales_summary.show()
Result:
region_name	total_sales
Ege	632
Marmara	5261
#### 2.2 Spark SQL Solution 💾
# Create TempViews
product_df.createOrReplaceTempView("product")
sale_df.createOrReplaceTempView("sale")
retailer_df.createOrReplaceTempView("retailer")
region_df.createOrReplaceTempView("region")

# SQL Query
query = """
SELECT r.region_name, SUM(s.quantity) AS total_sales
FROM sale s
JOIN product p ON s.product_id = p.product_id
JOIN retailer t ON s.retailer_id = t.retailer_id
JOIN region r ON t.city_id = r.city_id
WHERE p.product_type = 'Jacket'
GROUP BY r.region_name
"""

jacket_sales_summary_sql = spark.sql(query)
jacket_sales_summary_sql.show()

### 3. Assignment 2: Maximum Turnover by Seller Region
Identify the region with the highest total turnover (total_amt).
3.1 PySpark Solution 🐍
from pyspark.sql import functions as F

# Join DataFrames
sale_filtered_df = sale_df.select("retailer_id", "total_amt")
sales_retailer_df = sale_filtered_df.join(retailer_df.select("retailer_id", "city_id"), on="retailer_id", how="inner")
sales_retailer_region_df = sales_retailer_df.join(region_df.select("city_id", "region_name"), on="city_id", how="inner")

# Calculate total turnover per region
region_turnover_df = sales_retailer_region_df.groupBy("region_name").agg(F.sum("total_amt").alias("total_turnover"))

# Find region with maximum turnover
max_turnover_region_df = region_turnover_df.orderBy(F.desc("total_turnover")).limit(1)
max_turnover_region_df.show()
Result:
region_name	total_turnover
Marmara	635153
3.2 Spark SQL Solution 💾
# Create TempViews
sale_df.createOrReplaceTempView("sale")
retailer_df.createOrReplaceTempView("retailer")
region_df.createOrReplaceTempView("region")

# SQL Query
query = """
SELECT r.region_name, SUM(s.total_amt) AS total_turnover
FROM sale s
JOIN retailer rtr ON s.retailer_id = rtr.retailer_id
JOIN region r ON rtr.city_id = r.city_id
GROUP BY r.region_name
ORDER BY total_turnover DESC
LIMIT 1
"""

max_turnover_region_df = spark.sql(query)
max_turnover_region_df.show()
Results and Analysis 💡
Assignment 1 (Jacket Sales): Marmara region had the highest sales (5261 units), followed by Ege (632 units).
Assignment 2 (Maximum Turnover): Marmara region had the highest total turnover (635153).
Validation: All operations performed with PySpark were verified using Spark SQL queries.
