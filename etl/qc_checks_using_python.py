import logging
logging.basicConfig(level=logging.INFO)
logging.basicConfig(format="%(asctime)s:%(levelname)s:%(name)s:%(message)s",datefmt="%Y-%m-%d %H:%M:%s")
logger = logging.getLogger(__name__)

quality_control_alerts = {}

# Special Characters Check
def generate_special_characters_alert(pandas_df):
    try:

        special_characters_alerts = []
        special_characters_check_fails = []
        pandas_df['row_number'] = range(1, len(pandas_df) + 1)

        for column in pandas_df.columns:
            column_type = pandas_df[column].dtype
            regex_pattern = r"[\!\@\$\^\&\-\_\;\:\?\.\#\*\<\>]" if column == "ADDRESS" else r"[\!\@\$\^\&\-\_\;\:\?\.\#\*\<\>\,]"
            if column == "row_number":
                continue
            special_chars_df = pandas_df[pandas_df.filter(items=[column]).apply(lambda x: x.astype(str).str.contains(regex_pattern, regex=True)).any(axis=1)][['row_number',column]]
            special_characters_check_fails.append(len(special_chars_df))
            logger.info(column + " : " + str(len(special_chars_df)))
            if len(special_chars_df):
                unexpected_rows = special_chars_df.assign(COLUMN=column)
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

        country_check_alerts = []
        pandas_df['row_number'] = range(1, len(pandas_df) + 1)
        unexpected_countries_df = pandas_df[~pandas_df["COUNTRY"].eq(specific_value) | pandas_df["COUNTRY"].isna()][["row_number","COUNTRY"]]
        logger.info("Unexpected value counts" + " : " + str(len(unexpected_countries_df)))

        if len(unexpected_countries_df):
            unexpected_rows =  unexpected_countries_df.assign(COLUMN="COUNTRY")
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
        state_check_alerts = []
        pandas_df['row_number'] = range(1, len(pandas_df) + 1)
        unexpected_states_df = pandas_df[~pandas_df["STATE"].isin(specific_list) | pandas_df["STATE"].isna()][["row_number", "STATE"]]
        logger.info("Unexpected state observation counts" + " : " + str(len(unexpected_states_df)))

        if len(unexpected_states_df):
            unexpected_rows = unexpected_states_df.assign(COLUMN="STATE")
            state_check_alerts.extend(unexpected_rows[['row_number', 'COLUMN']].to_dict(orient='records'))

        quality_control_alerts["state_check"] = state_check_alerts
        logger.info("state_check_alerts : \n" + str(state_check_alerts[0:5]))
        pandas_df.drop(columns=['row_number'], inplace=True)
        quality_control_alerts["state_check"] = state_check_alerts
        logger.info(state_check_alerts[0:5])

    except Exception as e:
        logger.error('Exception e : ' + str(e))

# Not null and blank check : column should not be null or blank
def generate_null_or_blank_alert(pandas_df):
    try:
        null_or_blank_check_alerts = []
        pandas_df['row_number'] = range(1, len(pandas_df) + 1)
        for column in pandas_df.columns:
            if column == "row_number":
                continue
            column_type = pandas_df[column].dtype
            unexpected_rows_df = pandas_df[pandas_df[column].isna() | pandas_df[column].astype(str).eq("") ][["row_number", column]]
            logger.info("Null or blank observation counts for " + column +  " : " + str(len(unexpected_rows_df)))

            if len(unexpected_rows_df):
                unexpected_rows = unexpected_rows_df.assign(COLUMN=column)
                null_or_blank_check_alerts.extend(unexpected_rows[['row_number', 'COLUMN']].to_dict(orient='records'))

        quality_control_alerts["not_null_or_blank_check"] = null_or_blank_check_alerts
        logger.info("null_or_blank_check_alerts_count : " + str(len(quality_control_alerts["not_null_or_blank_check"])))
        logger.info(null_or_blank_check_alerts[0:5])
        pandas_df.drop(columns=['row_number'], inplace=True)
    except Exception as e:
        logger.error('Exception e : ' + str(e))