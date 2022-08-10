# Databricks notebook source
dbutils.widgets.text("FileName","")
filename=dbutils.widgets.get("FileName")

# COMMAND ----------

# MAGIC %md #### This notebook will ingest the bronze files

# COMMAND ----------

dbutils.notebook.run("/Users/yamini.akketi@tigeranalytics.com/POC/bronze/Ingest_bronze_files",60,{"FileName":filename})

# COMMAND ----------

# MAGIC %md #### This notebook will process the bronze files

# COMMAND ----------

if filename.startswith("cust"):
    dbutils.notebook.run("/Users/yamini.akketi@tigeranalytics.com/POC/silver/process_bronze_customers_data",60,{"FileName":filename})
elif filename.startswith("orde"):
    dbutils.notebook.run("/Users/yamini.akketi@tigeranalytics.com/POC/silver/process_bronze_orders_data",60,{"FileName":filename})
else:
    dbutils.notebook.run("/Users/yamini.akketi@tigeranalytics.com/POC/silver/process_bronze_payments_data",60,{"FileName":filename}) 

# COMMAND ----------

# MAGIC %md #### This notebook will write gold aggregated data to the adls location

# COMMAND ----------

dbutils.notebook.run("/Users/yamini.akketi@tigeranalytics.com/POC/gold/aggregations on gold data",60,{"FileName":filename})
