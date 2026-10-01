"""
A module for starting Spark Jobs
"""
from pyspark import SparkContext, SparkConf

from .logger import Log4py


def start_spark(
        number_cores: int = 2,
        memory_gb: int = 4,
        app_name: str = "sample_job",
        spark_config: dict[str, str] = {}) -> tuple[SparkContext, Log4py]:
    """
    Start Spark Context on the local node, get logger and load config if present.
    
    Args:
        number_cores (int): Number of workers to use
        memory_gb (int): Memory to use for running Spark
        app_name (str): Name of Spark app.
        spark_config (dict[str,str]): Dictionary of config key-value pairs.
    
    Returns:
        A tuple of references to the Spark session and logger.
    """
    conf = SparkConf().setAppName(app_name).setMaster(f"local[{number_cores}]")
    spark_config.update({"spark.driver.memory": f"{memory_gb}g"})

    for k, v in spark_config.items():
        conf.set(k, v)

    # Create a Spark Context object
    spark = SparkContext(conf=conf)
    logger = Log4py(spark)

    logger.info("Context Started.")

    return spark, logger
