import logging
import os
from pyspark.sql import SparkSession, dataframe
from pyspark.sql.types import StructType


def read_file(spark: SparkSession, input_path: str, schema: StructType) -> dataframe:
    """
    Read a input path into a pyspark dataframe.
    :param spark: Current SparkSession.
    :param input_path: Path to input file.
    :param schema: Schema for input file.
    :return: PySpark dataframe
    """
    if not os.path.exists(input_path):
        logging.error(f"File does not exist.\n{input_path}")
        exit(1)
    try:
        data = spark.read.csv(input_path, inferSchema=True, sep="::", schema=schema)
    except IOError as ioe:
        logging.error(f"Error reading file.\n {ioe}")
        exit(1)
    return data


def output_file(output_dataframe: dataframe, output_path: str):
    """
    Output dataframe to disk as parquet.
    :param output_dataframe: Dataframe to write to parquet
    :param output_path: Path to output to.
    :return: None
    """
    try:
        output_dataframe.write.parquet(output_path)
    except IOError as ioe:
        logging.error(f"Unable to write parquet due to an IOError.\n {ioe}")
        exit(1)
    except Exception as e:
        logging.error(f"Unable to write parquet.\n {e}")
        exit(1)
