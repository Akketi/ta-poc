# Databricks notebook source
# MAGIC %run "/Users/yamini.akketi@tigeranalytics.com/POC/config/poc_config"

# COMMAND ----------

dbutils.widgets.text("FileName","")
filename=dbutils.widgets.get("FileName")

# COMMAND ----------

from delta.tables import *
class silver_payments_data:
        
    def run(self):
        """
        This function calls all the functions in this class
        """
        try:
            self.payments_data_without_nulls(filename)
            self.write_and_merge_payments_files()
        
        except:
            print("Function which is called doesn't exist in the class")
        
    def payments_data_without_nulls(self,filename):
        """
        This function will drop all the null values and duplicate entries in the data
        """
        try:
            payments_data_df=spark.sql(f"select * from global_temp.bronze_{filename[:-4]}_glbvw")
            self.payments_data_without_nulls=payments_data_df.na.drop()
            self.payments_data_with_distinct_values=self.payments_data_without_nulls.dropDuplicates()
            self.payments_data_with_distinct_values.createOrReplaceGlobalTempView(f"silver_{filename[:-4]}_glbvw")
        
        except Exception as error:
            print(error)
        
    def write_and_merge_payments_files(self):
        """
        This function will do upsert on the target delta table
        """
        try:
            if DeltaTable.isDeltaTable(spark, gold_data+"payments")==True:
                delta_table=DeltaTable.forPath(spark,f"{gold_data}payments")
                delta_table.alias("base_data")\
                .merge(self.payments_data_without_nulls.alias("new_data"),"base_data.order_id=new_data.order_id")\
                .whenMatchedUpdate(set={"payment_sequential":"new_data.payment_sequential",
                                   "payment_type":"new_data.payment_type",
                                   "payment_installments":"new_data.payment_installments",
                                   "payment_value":"new_data.payment_value"})\
                .whenNotMatchedInsertAll()\
                .execute()
            else:
                self.payments_data_without_nulls.write.format("delta").mode("append").save(gold_data+"payments")
                spark.sql("create database if not exists gold")
                spark.sql(f"create table gold.payments using delta location '{gold_data}payments'")

        except Exception as error:
            print(error)

obj=silver_payments_data()
obj.run()

# COMMAND ----------

dbutils.notebook.exit("payments data processed successfully!")

# COMMAND ----------


