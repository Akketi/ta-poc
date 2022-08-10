# Databricks notebook source
# MAGIC %run "/Users/yamini.akketi@tigeranalytics.com/POC/config/poc_config"

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.text("FileName","")
filename=dbutils.widgets.get("FileName")

# COMMAND ----------

if filename.startswith("cust"):
    customers_df=spark.sql("select  * from gold.customers")
    customers_data_grouping_by_state=customers_df\
                                    .groupby("customer_state")\
                                    .count()\
                                    .withColumnRenamed('count','No_of_customers_state')
    customers_data_grouping_by_state=customers_data_grouping_by_state\
                                    .withColumn("dense_rank",F.dense_rank().over(F.Window.orderBy(F.desc("No_of_customers_state"))))
    customers_data_grouping_by_state.write.format("delta").mode("overwrite").save(gold_aggregate_data_customers)

elif filename.startswith("orde"):
    orders_df=spark.sql("select * from gold.orders")
    orders_df=orders_df.groupby("order_year_month").count().withColumnRenamed("count","no_of_orders").orderBy("count",ascending=False)
    orders_df.write.format("delta").mode("overwrite").save(gold_aggregate_data_orders)

else:
    payments_df=spark.sql("select * from gold.payments")
    payments_df=payments_df.groupby("payment_type").sum("payment_value").withColumnRenamed("sum(payment_value)","total_amount")
    payments_df.write.format("delta").mode("overwrite").save(gold_aggregate_data_payments)
    

# COMMAND ----------

payments_df=spark.sql("select * from gold.payments")
payments_df=payments_df.groupby("payment_type").sum("payment_value").withColumnRenamed("sum(payment_value)","total_amount")
display(payments_df)

# COMMAND ----------


