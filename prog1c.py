from pyspark.sql import SparkSession

spark = SparkSession. \
    builder. \
   	config('spark.shuffle.useOldFetchProtocol', 'true'). \
    config("spark.sql.warehouse.dir", "/user/itv014119/warehouse"). \
    enableHiveSupport(). \
    master('yarn'). \
    getOrCreate()

orders_schema = "order_id long, order_date string, cust_id long, order_status string"

orders_d = [
    (1,'2020-03-03',11,"COMPLETE"),
    (2,'2020-03-04',12,"CLOSED"),
    (3,'2020-03-04=5',55,"ON HOLD"),
    (4,'David',60,29),
]

orders_df = spark.createDataFrame(data=orders_d,schema=orders_schema)
#orders_df = spark.read \
#.format("csv") \
#.schema(orders_schema) \
#.load("/public/orders/orders_1gb.csv")

print(orders_df.rdd.getNumPartitions())

orders_df.createOrReplaceTempView("orders")

spark.sql("select order_status, count(*) as total from orders group by order_status").show()

spark.stop()



