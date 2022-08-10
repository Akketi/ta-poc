# Databricks notebook source
# MAGIC %run "/Users/yamini.akketi@tigeranalytics.com/POC/config/poc_config"

# COMMAND ----------

dbutils.widgets.text("FileName","")
filename=dbutils.widgets.get("FileName")

# COMMAND ----------

from delta.tables import *
class silver_customer_data:
        
    def run(self):
        """
        This function calls all the functions in this class
        """
        try:
            self.customer_data_without_nulls(filename)
            self.write_and_merge_files()
        except:
            print("Function which is called doesn't exist in the class")
        
    def customer_data_without_nulls(self,filename):
        """
        This function will drop all the null values and duplicate entries in the data
        """
        try:
            customer_data_df=spark.sql(f"select * from global_temp.bronze_{filename[:-4]}_glbvw")
            self.customer_data_without_nulls=customer_data_df.na.drop()
            self.distinct_customer_data=self.customer_data_without_nulls.dropDuplicates()
            self.distinct_customer_data.createOrReplaceGlobalTempView(f"silver_{filename[:-4]}_glbvw")
            
        except Exception as error:
            print(error)
        
    def write_and_merge_files(self):
        """
        This function will do upsert on the target delta table
        """
        try:
            if DeltaTable.isDeltaTable(spark, f"{gold_data}customers")==True:
                delta_table=DeltaTable.forPath(spark,f"{gold_data}customers")
                delta_table.alias("base_data")\
                .merge(self.customer_data_without_nulls.alias("new_data"),"base_data.customer_unique_id=new_data.customer_unique_id")\
                .whenMatchedUpdate(set={"customer_zip_code_prefix":"new_data.customer_zip_code_prefix",
                                   "customer_city":"new_data.customer_city",
                                   "customer_state":"new_data.customer_state",
                                   "customer_id":"new_data.customer_id"})\
                .whenNotMatchedInsertAll()\
                .execute()
            else:
                self.customer_data_without_nulls.write.format("delta").mode("append").save(f"{gold_data}customers")
                spark.sql("create database if not exists gold")
                spark.sql(f"create table if not exists gold.customers using delta location '{gold_data}customers'")
            
        except Exception as error:
            print(error)


obj=silver_customer_data()
obj.run()



# COMMAND ----------

dbutils.notebook.exit("customer data processed successfully!")
