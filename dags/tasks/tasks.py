import json
import logging
from datetime import datetime

from airflow.models import Variable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from tasks.utils import mock_api_call
from tasks.spark_session import create_spark_session

def load_raw_data(json_url: str, hdfs_path: str):
    """
    Consult and API, return its list of jsons and save them as a dataframe.

    Args:
        json_url (str): API to consult.
        hdfs_path (str): Path to store data on HDFS.

    Raises:
        ValueError: If json is not valid.
        Exception: Any error.
    """
    try:
        spark = create_spark_session()

        base_hdfs = Variable.get("base_path_hadoop")

        try:
            data = mock_api_call(json_url)

            print("Mock Data")
            print(data)

            rdd = spark.sparkContext.parallelize([data])
            df = spark.read.json(rdd)
        except Exception as e:
            raise ValueError(f"Error on processing the JSON: {e}")

        df.show(10)
        df.write.mode("overwrite").partitionBy("office").parquet(base_hdfs+hdfs_path)

        logging.info(f"Data loaded on HDFS: {base_hdfs+hdfs_path}")
    except Exception as e:
        logging.error(f"Error on processing: {e}")
        raise

# 2. Processamento dos Dados (Validação e Adição de Coluna de Data)
def validate_data(hdfs_input_path: str,validated_json_path_hdfs: str , json_config: str):
    """
    Read data from HDFS and add column if they are valid or not based on json_config.

    Args:
        hdfs_input_path (str): HDFS path of the data.
        hdfs_valid_path (str): HDFS path to save the validated data.
        json_config (str): Json with configuration to validate and transform.

    Raises:
        Exception: Any error.
    """
    try:
        spark = create_spark_session()
        base_hdfs = Variable.get("base_path_hadoop")
        df = spark.read.parquet(base_hdfs+hdfs_input_path)
        
        json_config = json.loads(json_config)
        for transformation in json_config.get("transformations", []):
            t_type = transformation.get("type")
            params = transformation.get("params", {})

            if t_type == "validate_fields":
                validations = params.get("validations", [])
                for valid in validations:
                    field = valid.get("field")
                    rules = valid.get("validations", [])
                    if field:
                        for rule in rules:
                            if rule == "notEmpty":
                                if field == "office":
                                    df = df.withColumn("office_valid", 
                                                       F.when((F.col(field).isNotNull()) & (F.col(field) != ""), True)
                                                        .otherwise(False))
                            elif rule == "notNull":
                                if field == "age": 
                                    df = df.withColumn("age_valid", 
                                                       F.when(F.col(field).isNotNull(), True)
                                                        .otherwise(False))

                df = df.withColumn("valid", F.col("office_valid") & F.col("age_valid"))
                
                # For each field that its not valid, add "field" and "error" on why.
                errors_expr = F.expr("""
                    filter(
                        array(
                            case when office_valid = false then named_struct('field', 'office', 'error', 'is empty') else null end,
                            case when age_valid = false then named_struct('field', 'age', 'error', 'is null') else null end
                        ),
                        x -> x is not null
                    )
                """)
                # Convert the array to json, if empty go null.
                df = df.withColumn("errors_raw", errors_expr) \
                       .withColumn("errors", 
                                   F.when(F.size(F.col("errors_raw")) > 0, F.to_json(F.col("errors_raw")))
                                    .otherwise(F.lit(None))) \
                       .drop("errors_raw")
            

            elif t_type == "add_fields":
                add_fields = params.get("addFields", [])
                for field_conf in add_fields:
                    field_name = field_conf.get("name")
                    function = field_conf.get("function")
                    if function == "current_timestamp":
                        df = df.withColumn("dt", F.current_timestamp())

        df = df.drop("office_valid", "age_valid")
        
        df.write.mode("overwrite").partitionBy("valid").parquet(base_hdfs+validated_json_path_hdfs)
        logging.info(f"Loaded on HDFS: {base_hdfs+validated_json_path_hdfs}")

    except Exception as e:
        logging.error(f"Error on processing: {e}")
        raise

# 3. Sincronizar os Dados com o Kafka
def transform_and_send(hdfs_valid_path: str, json_config: str):
    
    """
    Read data from HDFS and send to where the json_config defined to send.
    
    Args:
        hdfs_valid_path (str): path of the data on hdfs that we are going to work
        json_config(str): json formatted string with places we are going to send the data
        
    Raises:
        Exception: Any error on the code.
    """
    try:
        spark = create_spark_session()

        config = json.loads(json_config)

        base_hdfs = Variable.get("base_path_hadoop")
        df = spark.read.parquet(base_hdfs+hdfs_valid_path)

        # Valid or not
        df = df.withColumn("valid", F.col("valid").cast("boolean"))

        df_valid = df.filter(F.col("valid") == True)
        df_invalid = df.filter(F.col("valid") == False)

        # Sinks
        for sink in config.get("sinks", []):
            input_source = sink["input"]
            format_type = sink["format"]

            if format_type == "KAFKA":
                topic = sink["topics"][0]
                df_valid.selectExpr("to_json(struct(*)) AS value") \
                    .write \
                    .format("kafka") \
                    .option("kafka.bootstrap.servers", Variable.get("kafka_server")) \
                    .option("topic", topic) \
                    .save()
                logging.info(f"Valid data to Kafka topic: {topic}")

            elif format_type == "JSON":
                output_paths = sink["paths"]
                for path in output_paths:
                    df_invalid.write.mode("overwrite").json(base_hdfs+path)
                    logging.info(f"Invalid data saved on: {base_hdfs+path}")

        logging.info("Succesfull")

    except Exception as e:
        logging.error(f"Error on processing: {e}")
        raise

