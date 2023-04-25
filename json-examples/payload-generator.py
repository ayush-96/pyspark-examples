from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]"). \
    appName('payload-generator').getOrCreate()

data_path = '../resources/nested_json.json'
schema_df = spark.read.format("json").load(data_path)
schemaString = schema_df._jdf.schema().treeString()
print(schemaString)
# print(df.schema.json())
