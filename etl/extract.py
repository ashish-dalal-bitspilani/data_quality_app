from lib.logger import Log4j
import pandas as pd
import dask.dataframe as dd
def fetch_df(spark, **kwargs):

    """
    This is a generic function for extracting data from file, db, api sources
    :param spark: SparkSession variable(in case of spark) used for extracting data
    :param kwargs: has the details of the source, say for file, details like filepath, delimiter, header etc.
    :return: spark dataframe
    """
    logger = Log4j(spark)
    source_details = kwargs

    try:
        if source_details['source'] == "file":
            if source_details["library"] == "pyspark":
                df = spark.read.load(
                                        path=source_details['path'],
                                        format=source_details['format'],
                                        header=source_details['header'],
                                        inferSchema=source_details['inferSchema']
                )
                logger.info('File read successfully using ' + str(source_details["library"]))
                logger.info('Record count: ' + str(df.count()))
            elif source_details["library"] == "pandas":
                df = pd.read_csv(
                                        filepath_or_buffer=source_details['path'],
                                        header=source_details['header'],
                                        dtype=source_details['schema']
                )
                logger.info('File read successfully using ' + str(source_details["library"]))
                logger.info('Record count: ' + str(len(df)))
            elif source_details["library"] == "dask":
                df = dd.read_csv(
                                        urlpath=source_details['path'],
                                        header=source_details['header'],
                                        dtype=source_details['schema']
                )
                logger.info('File read successfully using ' + str(source_details["library"]))
                logger.info('Record count: ' + str(len(df)))

        elif source_details['source'] == 'db':
            pass
        elif source_details['source'] == 'api':
            pass
        else:
            pass
    except Exception as e:
        logger.error('Exception occurred : ' + str(e))
    return df