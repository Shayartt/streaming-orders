from pyspark.sql import SparkSession

def get_dbutils(spark):
    try:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)
    except ImportError:
        import IPython
        dbutils = IPython.get_ipython().user_ns["dbutils"]
    return dbutils


def get_secret_var(spark: SparkSession, secret_scope: str, secret_key: str):
    return get_dbutils(spark).secrets.get(scope=secret_scope, key=secret_key)