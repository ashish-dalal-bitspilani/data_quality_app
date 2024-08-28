from great_expectations.dataset import PandasDataset
import logging

logging.basicConfig(level=logging.INFO)
logging.basicConfig(format="%(asctime)s:%(levelname)s:%(name)s:%(message)s",datefmt="%Y-%m-%d %H:%M:%s")
logger = logging.getLogger(__name__)

quality_control_alerts = {}

def get_great_expectations_dataframe(pandas_df):
    try:
        return PandasDataset(pandas_df)
    except Exception as e:
        logger.error('Exception e : ' + str(e))

# Special Characters Check
def generate_special_characters_alert(pandas_df):
    try:
        ge_df = get_great_expectations_dataframe(pandas_df)
        special_characters_alerts = []
        special_characters_check_fails = []
        pandas_df['row_number'] = range(1, len(pandas_df) + 1)
        for column in pandas_df.columns:
            if column == "row_number":
                continue
            elif column == "ADDRESS":
                expectation_outcome = ge_df.expect_column_values_to_not_match_regex(column, r"[\!\@\$\^\&\-\_\;\:\?\.\#\*\<\>]", result_format="COMPLETE")
            else:
                expectation_outcome = ge_df.expect_column_values_to_not_match_regex(column, r"[\!\@\$\^\&\-\_\;\:\?\.\#\*\<\>\,]", result_format="COMPLETE")
            special_characters_check_fails.append(expectation_outcome["result"]["unexpected_count"])
            logger.info(column + " : " + str(expectation_outcome["result"]["unexpected_count"]))
            if expectation_outcome["result"]["unexpected_count"]:
                unexpected_rows = pandas_df.iloc[expectation_outcome["result"]["unexpected_index_list"]].assign(COLUMN=column)
                special_characters_alerts.extend(unexpected_rows[['row_number', 'COLUMN']].to_dict(orient='records'))
        logger.info("total_check_fails : " + str(sum(special_characters_check_fails)))
        logger.info("special_character_alerts_count : " + str(len(special_characters_alerts)))
        logger.info("special_character_alerts : \n" + str(special_characters_alerts[0:5]))
        quality_control_alerts["special_characters_check"] =  special_characters_alerts
        pandas_df.drop(columns=['row_number'], inplace=True)
    except Exception as e:
        logger.error('Exception e : ' + str(e))

# Country Check : column to contain only 'India'
def generate_country_alert(pandas_df, specific_value):
    try:
        ge_df = get_great_expectations_dataframe(pandas_df)
        country_check_alerts = []
        pandas_df['row_number'] = range(1, len(pandas_df) + 1)
        expectation_outcome = ge_df.expect_column_values_to_be_in_set(column = "COUNTRY", value_set=[specific_value], result_format="COMPLETE")
        logger.info("Unexpected value counts" + " : " + str(expectation_outcome["result"]["unexpected_count"]))
        if expectation_outcome["result"]["unexpected_count"]:
            unexpected_rows = pandas_df.iloc[expectation_outcome["result"]["unexpected_index_list"]]\
                                       .assign(COLUMN="COUNTRY")
            country_check_alerts.extend(unexpected_rows[['row_number', 'COLUMN']].to_dict(orient='records'))
            logger.info("country_check_alerts_count : " + str(len(country_check_alerts)))

        quality_control_alerts["country_check"] = country_check_alerts
        logger.info("country_check_alerts : \n" + str(country_check_alerts[0:5]))
        pandas_df.drop(columns=['row_number'], inplace=True)
    except Exception as e:
        logger.error('Exception e : ' + str(e))

# State Check : column to contain only ['Haryana', 'Karnataka', 'Maharashtra']
def generate_state_alert(pandas_df, specific_list):
    try:
        ge_df = get_great_expectations_dataframe(pandas_df)
        state_check_alerts = []
        pandas_df['row_number'] = range(1, len(pandas_df) + 1)
        expectation_outcome = ge_df.expect_column_values_to_be_in_set(column="STATE", value_set=specific_list,\
                                                                      result_format="COMPLETE")
        expectation_outcome_for_nulls = ge_df.expect_column_values_to_not_be_null(column="STATE",\
                                                                      result_format="COMPLETE")
        logger.info("Unexpected value counts" + " : " + str(expectation_outcome["result"]["unexpected_count"]))
        logger.info("Unexpected null value counts" + " : " + str(expectation_outcome_for_nulls["result"]["unexpected_count"]))

        if expectation_outcome["result"]["unexpected_count"]:
            unexpected_rows = pandas_df.iloc[expectation_outcome["result"]["unexpected_index_list"]] \
                .assign(COLUMN="STATE")
            state_check_alerts.extend(unexpected_rows[['row_number', 'COLUMN']].to_dict(orient='records'))
            logger.info("state_check_unexpected_value_alerts_count : " + str(len(state_check_alerts)))

        if expectation_outcome_for_nulls["result"]["unexpected_count"]:
            unexpected_null_rows = pandas_df.iloc[expectation_outcome_for_nulls["result"]["unexpected_index_list"]] \
                                            .assign(COLUMN="STATE")
            state_check_alerts.extend(unexpected_null_rows[['row_number', 'COLUMN']].to_dict(orient='records'))
            logger.info("state_check_total_alerts_count : " + str(len(state_check_alerts)))


        quality_control_alerts["state_check"] = state_check_alerts
        pandas_df.drop(columns=['row_number'], inplace=True)
        quality_control_alerts["state_check"] = state_check_alerts
        logger.info(state_check_alerts[0:5])
    except Exception as e:
        logger.error('Exception e : ' + str(e))

# Not null and blank check : column should not be null or blank
def generate_null_or_blank_alert(pandas_df):
    try:
        ge_df = get_great_expectations_dataframe(pandas_df)
        null_or_blank_check_alerts = []
        pandas_df['row_number'] = range(1, len(pandas_df) + 1)
        for column in pandas_df.columns:
            if column == "row_number":
                continue
            column_type = pandas_df[column].dtype
            result_set_for_nulls = ge_df.expect_column_values_to_not_be_null(column=column, result_format="COMPLETE")
            unexpected_rows = pandas_df.iloc[result_set_for_nulls["result"]["unexpected_index_list"]] \
                                       .assign(COLUMN=column)
            null_or_blank_check_alerts.extend(unexpected_rows[['row_number', 'COLUMN']].to_dict(orient='records'))
            logger.info('Null count for ' + column + " : " + str(result_set_for_nulls["result"]["unexpected_count"]))
            if column_type == "object" or column_type == "str":
                result_set_for_blanks = ge_df.expect_column_value_lengths_to_be_between(column=column, min_value=1, result_format="COMPLETE")
                unexpected_blank_rows = pandas_df.iloc[result_set_for_blanks["result"]["unexpected_index_list"]] \
                                                 .assign(COLUMN=column)
                null_or_blank_check_alerts.extend(unexpected_blank_rows[['row_number', 'COLUMN']].to_dict(orient='records'))
                logger.info('Blank count for ' + column + " : " + str(result_set_for_blanks["result"]["unexpected_count"]))

        quality_control_alerts["not_null_or_blank_check"] = null_or_blank_check_alerts
        logger.info("null_or_blank_check_alerts_count : " + str(len(quality_control_alerts["not_null_or_blank_check"])))
        logger.info(null_or_blank_check_alerts[0:5])
        pandas_df.drop(columns=['row_number'], inplace=True)
    except Exception as e:
        logger.error('Exception e : ' + str(e))