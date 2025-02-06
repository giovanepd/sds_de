from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("Exemple_spark_submit") \
        .getOrCreate()

    print("Spark connectec sucessfully")

    dados = [("Alice", 1), ("Bob", 2), ("Catherine", 3)]
    colunas = ["name", "age"]
    df = spark.createDataFrame(dados, colunas)

    print("Original dataFrame :")
    df.show()

    df_transformado = df.withColumn("squared_age", df["age"] * df["age"])

    print("DataFrame:")
    df_transformado.show()

    spark.stop()
    print("Spark finalized.")

if __name__ == '__main__':
    main()