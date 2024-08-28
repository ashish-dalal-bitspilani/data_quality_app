from etl.qc_checks_using_pyspark_with_ge import generate_special_characters_qc_alert
from lib import spark_app_ops
from etl import extract, qc_checks_using_pyspark_with_ge, qc_checks_using_pyspark, qc_checks_using_python
from etl import qc_checks_using_python_with_ge
from lib.logger import Log4j

if __name__ == '__main__':
    try:
        spark, sc = spark_app_ops.start_spark_app("data_quality_app")
        logger = Log4j(spark)
        employee_df = extract.fetch_df(spark, source='file', path='data/employees.csv', format='csv', delimiter=',', header=True, inferSchema=True)
        qc_checks_using_pyspark_with_ge.generate_special_characters_qc_alert(spark, employee_df)
        qc_checks_using_pyspark_with_ge.generate_country_alert(spark,employee_df,"India")
        qc_checks_using_pyspark_with_ge.generate_state_alert(spark,employee_df,['Haryana', 'Karnataka', 'Maharashtra'])
        qc_checks_using_pyspark_with_ge.generate_null_or_blank_alert(spark, employee_df)
        qc_checks_using_pyspark.generate_special_characters_qc_alert(spark, employee_df)
        qc_checks_using_pyspark.generate_country_alert(spark,employee_df,"India")
        qc_checks_using_pyspark.generate_state_alert(spark,employee_df,['Haryana', 'Karnataka', 'Maharashtra'])
        qc_checks_using_pyspark.generate_null_or_blank_alert(spark, employee_df)
        employee_pandas_df = extract.fetch_df_using_pandas(source='file', path='data/employees.csv', header=0, schema={'EMP_ID': 'int32', 'FIRST_NAME': 'str', 'LAST_NAME': 'str', 'ADDRESS':'str', 'CITY':'str','STATE':'str', 'COUNTRY':'str'})
        qc_checks_using_python_with_ge.generate_special_characters_alert(employee_pandas_df)
        qc_checks_using_python_with_ge.generate_country_alert(employee_pandas_df,'India')
        qc_checks_using_python_with_ge.generate_state_alert(employee_pandas_df, ['Haryana', 'Karnataka', 'Maharashtra'])
        qc_checks_using_python_with_ge.generate_null_or_blank_alert(employee_pandas_df)
        qc_checks_using_python.generate_special_characters_alert(employee_pandas_df)
        qc_checks_using_python.generate_country_alert(employee_pandas_df,"India")
        qc_checks_using_python.generate_state_alert(employee_pandas_df, ['Haryana', 'Karnataka', 'Maharashtra'])
        qc_checks_using_python.generate_null_or_blank_alert(employee_pandas_df)
    except Exception as e:
        logger.error('Exception e : ' + str(e))
    else:
        logger.info("QC checks completed smoothly")
    finally:
        spark_app_ops.close_spark_app(spark, sc)