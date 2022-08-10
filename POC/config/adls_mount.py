# Databricks notebook source
# MAGIC %run "/Users/yamini.akketi@tigeranalytics.com/POC/config/poc_config"

# COMMAND ----------

adls_accesskey=dbutils.secrets.get("accesskey","adls-accesskey")

# COMMAND ----------

def adls_mount():
    """
    This function mounts the data from azure data lake storage
    """
    try:
        dbutils.fs.mount(
        source=f"wasbs://{container_name}@{storage_acc_name}.blob.core.windows.net",
        mount_point="/mnt/poc/",
        extra_configs={f"fs.azure.account.key.{storage_acc_name}.blob.core.windows.net":{adls_accesskey}})
        print("Mount point created successfully!")
    except:
        print("Mount point already exists in the location you specified. Kindly change the mount point location")

adls_mount()

# COMMAND ----------

dbutils.notebook.exit("adls_mount notebook ran succesfully!")

# COMMAND ----------


