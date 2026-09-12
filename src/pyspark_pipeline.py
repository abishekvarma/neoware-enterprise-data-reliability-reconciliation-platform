# Production mapping skeleton
from pyspark.sql import functions as F
def standardize_orders(df):
    return df.withColumn("order_amount",F.col("order_amount").cast("double")).withColumn("order_date",F.to_date("order_date")).dropDuplicates(["order_id"])
def reconcile(orders,invoices):
    return orders.groupBy("order_id").agg(F.sum("order_amount").alias("order_amount")).join(invoices.groupBy("order_id").agg(F.sum("invoice_amount").alias("invoice_amount")),"order_id","full").fillna(0,["order_amount","invoice_amount"]).withColumn("difference",F.round(F.col("order_amount")-F.col("invoice_amount"),2))
