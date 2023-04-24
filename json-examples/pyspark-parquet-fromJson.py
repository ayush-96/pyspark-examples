from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, to_date, row_number
from pyspark.sql.window import Window

spark = SparkSession.builder.master("local[1]").\
    appName("Json-parquet").getOrCreate()

data_path = "../resources/jobs.parquet"

df = spark.read.format("parquet").load(data_path)
date_df = df.withColumn("json_load_ts", lit(current_timestamp()))
date_df = date_df.withColumn("json_load_dt", to_date(col("json_load_ts")))
window = Window.partitionBy(col("Product Name"), col("json_load_dt")).orderBy(col("json_load_ts").desc())
date_df = date_df.withColumn("rank", row_number().over(window)).\
    filter(col("rank") == 1).drop(col("rank"))
date_df.show()
# date_df.write.mode("overwrite").parquet("../resources/jobs1.parquet")
