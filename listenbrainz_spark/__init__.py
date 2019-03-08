from pyspark.sql import SparkSession

session = None

def init_spark_session(app_name):
    global session, context, sql_context
    session = SparkSession \
            .builder \
            .appName(app_name) \
            .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
            .config("spark.hadoop.dfs.datanode.use.datanode.hostname", "true") \
            .getOrCreate()
  
