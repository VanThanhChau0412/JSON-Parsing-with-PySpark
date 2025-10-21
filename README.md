# 🧩 Spark JSON Parser for Cloud Metrics

**Parse, flatten, and analyze deeply nested JSON datasets from S3 using PySpark — dynamically.**

This project provides a **schema-aware Spark parser** capable of recursively navigating and flattening complex JSON structures (with nested structs and arrays) to extract and compute meaningful cloud metrics such as **AWS**, **Azure**, and **Reachability** statistics.

---

## 🚀 Features

✅ **Recursive Schema Traversal**
Automatically locates any target field (like `"aws"`, `"reachable"`, or `"azure"`) across deeply nested arrays and structs.

✅ **Dynamic DataFrame Extraction**
Generates custom extraction pipelines on the fly, using `explode_outer` and structural projection to flatten nested layers.

✅ **Multi-Metric Analysis**
Aggregates and summarizes key metrics across multiple JSON files, building consistent analytical reports in one pass.

✅ **Robust Null/Empty Handling**
Differentiates between missing, null, and empty array data using schema inspection and PySpark type checks.

✅ **S3-Ready**
Designed for integration with Databricks or Spark environments reading from S3 (`dbutils.fs.ls` + `spark.read.json`).

---

## ☁️ Running on Databricks

This project is built to run **natively on Databricks**, leveraging its **pre-configured Spark session**, optimized **cluster environment**, and direct **S3 integration**.
By executing within Databricks notebooks, you can immediately access the `spark` session and `dbutils` utilities — eliminating the need for manual Spark initialization or AWS authentication setup.
This makes it ideal for production pipelines and large-scale exploratory analysis of cloud telemetry or configuration data stored in JSON format.

---

## 🧠 How It Works

The project has three main components:

1. **`find_field_paths()`**
   Recursively walks a `StructType` schema to find all possible paths leading to a target field.

2. **`make_extractor()`**
   Dynamically constructs a Spark DataFrame transformation that expands structs and arrays (via `explode_outer`) until it isolates the desired column.

3. **`process_df_multi_metrics()`**
   Iterates through multiple target metrics, extracts them using the extractor, and builds a consolidated JSON-style summary report:

   ```json
   {
     "group_id": "abc123",
     "analytics": {
       "vulnerabilities": {
         "aws_account": 42,
         "azure_account": 15
       }
     },
     "networking": {
       "operational": {
         "count": 27,
         "abc": 12,
         "bcd": 8
       },
       "non_operational_total": 5
     }
   }
   ```

---

## 🧩 Example Usage

```python
# 1. Load JSON files from S3
s3_folder = "s3://your-bucket/data/"
json_files = [f.path for f in dbutils.fs.ls(s3_folder) if f.path.endswith('.json')]
dataframes = [spark.read.option("multiline", "true").json(file) for file in json_files]

# 2. Process a single file
result = process_df_multi_metrics(dataframes[0])
print(result)
```

---

## 📊 Typical Use Cases

* Cloud inventory and reachability analysis
* Vulnerability or compliance dashboards
* Large-scale JSON data flattening in Spark
* Exploratory schema discovery and ETL automation

---

## 🏗️ Architecture Overview

```text
┌───────────────────────┐
│  S3 JSON Data Files   │
└────────────┬──────────┘
             │
             ▼
   ┌──────────────────┐
   │  find_field_paths │  ← Recursively finds all field paths
   └────────┬─────────┘
            │
            ▼
   ┌──────────────────┐
   │   make_extractor │  ← Flattens arrays/structs dynamically
   └────────┬─────────┘
            │
            ▼
   ┌────────────────────────┐
   │  process_df_multi_metrics │  ← Counts metrics & builds JSON report
   └────────────────────────┘
```

---

## 🧰 Requirements

* **Python 3.8+**
* **Apache Spark 3.x**
* **Databricks Runtime environment (recommended)**
* Access to **S3** or compatible storage

---

## ⚙️ Extending the Parser

You can add new metrics simply by listing them in the `metrics` argument:

```python
process_df_multi_metrics(df, metrics=["aws", "azure", "reachable", "abc", "bcd", "gcp"])
```

The parser automatically discovers the correct path to each field.

---

## 👨‍💻 Author

**Van Châu Thanh**
