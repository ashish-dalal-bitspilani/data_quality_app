from lib.logger import Log4j
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from great_expectations.dataset import SparkDFDataset, MetaSparkDFDataset

quality_control_alerts = {}

def get_spark_df_with_row_number(spark, spark_df):
    try:
        logger = Log4j(spark)
        window_spec = Window.orderBy(spark_df.columns[0])
        df_with_row_number = spark_df.withColumn("row_number", F.row_number().over(window_spec))
        return df_with_row_number
    except Exception as e:
        logger.error('Exception e : ' + str(e))

def get_great_expectations_dataframe(spark, spark_df):
    try:
        logger = Log4j(spark)
        df_with_row_number = get_spark_df_with_row_number(spark, spark_df)
        return SparkDFDataset(df_with_row_number)
    except Exception as e:
        logger.error('Exception e : ' + str(e))

# Special Characters Check : Defining Expectation
def expect_column_values_to_not_have_special_characters_except_commas(self, spark, spark_df, column):
    try:
        logger = Log4j(spark)
        spark_df = get_spark_df_with_row_number(spark, spark_df)
        logger.info("Regular expression to match special characters")
        regex_pattern = r"[\!\@\$\^\&\-\_\;\:\?\.\#\*\<\>]" if column == "ADDRESS" else r"[\!\@\$\^\&\-\_\;\:\?\.\#\*\<\>\,]"

        logger.info("Applying the regex to check for special characters in " + column)
        special_character_rows = spark_df.filter(spark_df[column].rlike(regex_pattern))
        special_character_rows = special_character_rows.select("row_number").collect()
        logger.info(column + ": " + str(len(special_character_rows)))
        return special_character_rows
    except Exception as e:
        logger.error('Exception e : ' + str(e))


# Special Characters Check : Generating QC Alert
def generate_special_characters_qc_alert(spark, spark_df):
    try:
        logger = Log4j(spark)
        MetaSparkDFDataset.expect_column_values_to_not_have_special_characters_except_commas = expect_column_values_to_not_have_special_characters_except_commas
        df_ge = get_great_expectations_dataframe(spark, spark_df)
        logger.info("Initialize the alert list")
        special_characters_alerts = []
        logger.info("Run the custom expectation on each column")
        for col in spark_df.columns:
            if col == "row_number":
                continue
            special_character_rows = df_ge.expect_column_values_to_not_have_special_characters_except_commas(spark, spark_df, col)
            for row in special_character_rows:
                special_characters_alerts.append({"row_number":row['row_number'], "COLUMN":col})

        logger.info("Prepare the final JSON output")
        quality_control_alerts["special_characters_check"] =  special_characters_alerts
        logger.info("special_character_alerts_count : "+ str(len(quality_control_alerts["special_characters_check"])))
        logger.info(special_characters_alerts[0:5])
    except Exception as e:
        logger.error('Exception e : ' + str(e))

# Country Check : column to contain only 'India'
def generate_country_alert(spark, spark_df, specific_value):
    try:
        logger = Log4j(spark)
        ge_df = get_great_expectations_dataframe(spark, spark_df)
        result_set = ge_df.expect_column_values_to_be_in_set(column="COUNTRY", value_set=[specific_value])
        logger.info(str(result_set))
        result_set_for_nulls = ge_df.expect_column_values_to_not_be_null(column="COUNTRY")
        logger.info(str(result_set_for_nulls))
        spark_df = get_spark_df_with_row_number(spark, spark_df)
        filtered_df = spark_df.filter(~spark_df["COUNTRY"].isin([specific_value]) | spark_df["COUNTRY"].isNull())
        country_check_alerts = [{"row_number": row.row_number, "COLUMN": "COUNTRY"} for row in filtered_df.collect()]
        quality_control_alerts["country_check"] = country_check_alerts
        logger.info("country_check_alerts_count : " + str(len(quality_control_alerts["country_check"])))
        logger.info("country_check_alerts_count : " + str(result_set["result"]["unexpected_count"] + \
                                                          result_set_for_nulls["result"]["unexpected_count"]))
        logger.info('country_check_null_count : ' + str(result_set_for_nulls["result"]["unexpected_count"]))
        logger.info(country_check_alerts[0:5])
    except Exception as e:
        logger.error('Exception e : ' + str(e))

# State Check : column to contain only ['Haryana', 'Karnataka', 'Maharashtra']
def generate_state_alert(spark, spark_df, specific_list):
    try:
        logger = Log4j(spark)
        ge_df = get_great_expectations_dataframe(spark, spark_df)
        result_set = ge_df.expect_column_values_to_be_in_set(column="STATE", value_set=specific_list)
        result_set_for_nulls = ge_df.expect_column_values_to_not_be_null(column="STATE")
        spark_df = get_spark_df_with_row_number(spark, spark_df)
        filtered_df = spark_df.filter(~spark_df["STATE"].isin(specific_list) | spark_df["STATE"].isNull())
        state_check_alerts = [{"row_number": row.row_number, "COLUMN": "STATE"} for row in filtered_df.collect()]
        quality_control_alerts["state_check"] = state_check_alerts
        logger.info("state_check_alerts_count : " + str(len(quality_control_alerts["state_check"])))
        logger.info("state_check_alerts_count : " + str(result_set["result"]["unexpected_count"] + \
                                                        result_set_for_nulls["result"]["unexpected_count"]))
        logger.info('state_check_null_count : ' + str(result_set_for_nulls["result"]["unexpected_count"]))
        logger.info(state_check_alerts[0:5])
    except Exception as e:
        logger.error('Exception e : ' + str(e))

# Not null and blank check : column should not be null or blank
def generate_null_or_blank_alert(spark, spark_df):
    try:
        logger = Log4j(spark)
        ge_df = get_great_expectations_dataframe(spark, spark_df)
        spark_df = get_spark_df_with_row_number(spark, spark_df)
        null_or_blank_check_alerts = []
        for column in spark_df.columns:
            if column == "row_number":
                continue
            column_type = dict(spark_df.dtypes)[column]
            result_set_for_nulls = ge_df.expect_column_values_to_not_be_null(column=column)
            logger.info('Null count for ' + column + " : " +str(result_set_for_nulls["result"]["unexpected_count"]))
            if column_type.startswith("string"):
                result_set_for_blanks = ge_df.expect_column_value_lengths_to_be_between(column, min_value=1)
                logger.info('Blank count for ' + column + " : " + str(result_set_for_blanks["result"]["unexpected_count"]))
            filtered_df = spark_df.filter(spark_df[column].isNull() | (F.trim(F.col(column)) == ""))
            null_or_blank_check_alerts.extend([{"row_number": row.row_number, "COLUMN": column} for row in filtered_df.collect()])
        quality_control_alerts["not_null_or_blank_check"] = null_or_blank_check_alerts
        logger.info("null_or_blank_check_alerts_count : " + str(len(quality_control_alerts["not_null_or_blank_check"])))
        logger.info(null_or_blank_check_alerts[0:5])
    except Exception as e:
        logger.error('Exception e : ' + str(e))