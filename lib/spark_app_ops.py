from pyspark.sql import SparkSession
from lib.logger import Log4j

def start_spark_app(app_name, **kwargs):

    spark = SparkSession\
        .builder \
        .master('local[*]')\
        .appName(app_name)\
        .getOrCreate()
    sc = spark.sparkContext

    logger = Log4j(spark)
    logger.info('SparkSession variable : '+ str(spark))
    logger.info('sparkContext variable : '+ str(sc))

    #df = spark.read.load('data/employees.csv', format="csv", header=True, inferSchema=True).cache()
    #logger.info('observation set: '+ str(df.count()))

    #df = spark.read.csv('data/employees.csv', header=True, inferSchema=True).cache()
    #logger.info('observation set: '+ str(df.count()))

    return spark, sc

def close_spark_app(spark, sc):
    spark.stop()
    sc.stop()
    logger = Log4j(spark)
    logger.info("Closed spark session and spark context")