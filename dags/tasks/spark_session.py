from airflow.models import Variable
from pyspark.sql import SparkSession

def create_spark_session(app_name='SparkApp', master=Variable.get("spark_server")):
    try:
        spark = SparkSession.builder \
            .appName(app_name) \
            .master(master) \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
            .getOrCreate()
        print(f"Conexão com Spark estabelecida com sucesso: {app_name}")
        return spark
    except Exception as e:
        print(f"Erro ao conectar com o Spark: {e}")
        raise