# Databricks notebook source
# MAGIC %run "/Users/yamini.akketi@tigeranalytics.com/POC/config/poc_config"

# COMMAND ----------

dbutils.widgets.text("FileName","")
filename=dbutils.widgets.get("FileName")

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import *
class silver_orders_data:
        
    def run(self):
        """
        This function calls all the functions in this class
        """
        try:
            self.orders_data_without_nulls(filename)
            self.manipulating_orders_data()
            self.write_and_merge_orders_files()
            
        except:
            print("Function which is called doesn't exist in the class")
        
    def orders_data_without_nulls(self,filename):
        """
        This function will drop all the null values and duplicates entries in the data
        """
        try:
            orders_data_df=spark.sql(f"select * from global_temp.bronze_{filename[:-4]}_glbvw")
            self.orders_data_without_nulls=orders_data_df.na.drop() 
            self.orders_data_with_distinct_values=self.orders_data_without_nulls.dropDuplicates()
            self.orders_data_with_distinct_values.createOrReplaceGlobalTempView(f"silver_{filename[:-4]}_glbvw")
        
        except Exception as error:
            print(error)
        
    def manipulating_orders_data(self):
        """
        This function will cast the datatype of the columns as required
        """
        try:
            self.orders_data=self.orders_data_without_nulls.withColumn('order_purchase_timestamp',F.to_timestamp('order_purchase_timestamp',
                                                                                                             format='dd-MM-yyyy HH:mm'))\
            .withColumn('order_delivered_carrier_data',F.to_timestamp('order_delivered_carrier_date',format='dd-MM-yyyy HH:mm'))\
            .withColumn('order_approved_at',F.to_timestamp('order_approved_at',format='dd-MM-yyyy HH:mm'))\
            .withColumn('order_delivered_customer_date',F.to_timestamp('order_delivered_customer_date',format='dd-MM-yyyy HH:mm'))\
            .withColumn('order_estimed_delivery_date',F.to_timestamp('order_estimated_delivery_date',format='dd-MM-yyyy HH:mm'))\
            .withColumn("order_year_month", F.date_format(F.col("order_purchase_timestamp"), format='yyyy-MM'))
            
        except Exception as error:
            print(error)
    
    def write_and_merge_orders_files(self):
        """
        This function will do upsert on the target delta table
        """
        try:
            if DeltaTable.isDeltaTable(spark, gold_data+"orders")==True:
                delta_table=DeltaTable.forPath(spark,f"{gold_data}orders")
                delta_table.alias("base_data")\
                .merge(self.orders_data_without_nulls.alias("new_data"),"base_data.order_id=new_data.order_id")\
                .whenMatchedUpdate(set={"customer_id":"new_data.customer_id",
                                   "order_status":"new_data.order_status",
                                   "order_purchase_timestamp":"new_data.order_purchase_timestamp",
                                   "order_approved_at":"new_data.order_approved_at",
                                   "order_delivered_carrier_date":"new_data.order_delivered_carrier_date",
                                   "order_delivered_customer_date":"new_data.order_delivered_customer_date",
                                   "order_estimated_delivery_date":"new_data.order_estimated_delivery_date"})\
                .whenNotMatchedInsertAll()\
                .execute()
            else:
                self.orders_data.write.format("delta").mode("append").save(gold_data+"orders")
                spark.sql("create database if not exists gold")
                spark.sql(f"create table if not exists gold.orders using delta location '{gold_data}orders'")
                
        except Exception as error:
            print(error)

            

obj=silver_orders_data()
obj.run()

# COMMAND ----------

dbutils.notebook.exit("orders data processed successfully!")
