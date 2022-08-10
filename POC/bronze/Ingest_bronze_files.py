# Databricks notebook source
# MAGIC %run "/Users/yamini.akketi@tigeranalytics.com/POC/config/poc_config"

# COMMAND ----------

dbutils.widgets.text("FileName","")
filename=dbutils.widgets.get("FileName")

# COMMAND ----------

def read_data(filename):
    """
    This function reads the data in the given path and creates a global view
    """
    try:
        spark.read.format("csv")\
        .option("header", "true")\
        .option("badRecordsPath",badrecordspath+filename)\
        .option("inferSchema",True)\
        .option("header",True)\
        .load(bronze_data+filename)\
       .createOrReplaceGlobalTempView(f"bronze_{filename[:-4]}_glbvw")
        
    except Exception as error:
        print(error)
read_data(filename)      

# COMMAND ----------

dbutils.notebook.exit("bronze files ingested succesfully!")
