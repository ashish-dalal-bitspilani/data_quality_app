import logging
logger = logging.getLogger(__name__)
logging.basicConfig(format="%(asctime)s:%(levelname)s:%(name)s:%(lineno)s - %(funcName)s:%(message)s",datefmt="%Y-%m-%d %H:%M:%s")
logging.basicConfig(level=logging.INFO)

quality_control_alerts = {}

# Special Characters Check
def generate_special_characters_alert(dask_df):
    try:
        special_characters_alerts = []
        special_characters_check_fails = []
        dask_df = dask_df.assign(row_number=(dask_df.reset_index().index + 1))
        columns = dask_df.columns
        for column in columns:
            regex_pattern = r"[\!\@\$\^\&\-\_\;\:\?\.\#\*\<\>]" if column == "ADDRESS" else r"[\!\@\$\^\&\-\_\;\:\?\.\#\*\<\>\,]"
            if column == "row_number":
                logger.info("Skipping check for : " + column)
                continue
            special_chars_rows = dask_df[dask_df[column].astype(str).str.contains(regex_pattern, regex=True)][['row_number']]
            special_chars_row_nums = special_chars_rows.compute()
            special_characters_check_fails.append(len(special_chars_row_nums))
            logger.info("Special Chars Check Fails for " + column + " : " + str(len(special_chars_row_nums)))
            for row in special_chars_row_nums['row_number']:
                special_characters_alerts.append(
                    {
                        'row_number' : row,
                        'COLUMN' : column
                    }
                )
        logger.info("total_check_fails : " + str(sum(special_characters_check_fails)))
        logger.info("special_character_alerts_count : " + str(len(special_characters_alerts)))
        quality_control_alerts["special_characters_check"] = special_characters_alerts
        logger.info("special_character_alerts : " + str(special_characters_alerts[0:5]))
    except Exception as e:
        logger.error('Exception e : ' + str(e))

# Country Check : column to contain only 'India'
def generate_country_alert(dask_df, specific_value):
    try:

        country_check_alerts = []
        dask_df = dask_df.assign(row_number=(dask_df.reset_index().index + 1))
        unexpected_countries_df = dask_df[~dask_df["COUNTRY"].eq(specific_value) | dask_df["COUNTRY"].isna()][["row_number"]]
        logger.info("Unexpected value counts" + " : " + str(len(unexpected_countries_df)))
        unexpected_countries_row_nums = unexpected_countries_df.compute()

        for row in unexpected_countries_row_nums['row_number']:
            country_check_alerts.append(
                {
                    'row_number' : row,
                    'COLUMN' : "COUNTRY"
                }
            )

        logger.info("country_check_alerts_count : " + str(len(country_check_alerts)))
        quality_control_alerts["country_check"] = country_check_alerts
        logger.info("country_check_alerts : \n" + str(country_check_alerts[0:5]))
    except Exception as e:
        logger.error('Exception e : ' + str(e))


# State Check : column to contain only ['Haryana', 'Karnataka', 'Maharashtra']
def generate_state_alert(dask_df, specific_list):
    try:
        state_check_alerts = []
        dask_df = dask_df.assign(row_number=(dask_df.reset_index().index + 1))
        unexpected_states_df = dask_df[~dask_df["STATE"].isin(specific_list) | dask_df["STATE"].isna()][["row_number"]]
        logger.info("Unexpected state observation counts" + " : " + str(len(unexpected_states_df)))

        unexpected_states_row_nums = unexpected_states_df.compute()
        for row in unexpected_states_row_nums['row_number']:
            state_check_alerts.append(
                {
                    'row_number': row,
                    'COLUMN': "STATE"
                }
            )

        quality_control_alerts["state_check"] = state_check_alerts
        logger.info(str(state_check_alerts[0:5]))
    except Exception as e:
        logger.error('Exception e : ' + str(e))

# Not null and blank check : column should not be null or blank
def generate_null_or_blank_alert(dask_df):
    try:
        null_or_blank_check_alerts = []
        dask_df = dask_df.assign(row_number=(dask_df.reset_index().index + 1))
        for column in dask_df.columns:
            if column == "row_number":
                logger.info("Skipping check for : " + column)
                continue
            unexpected_rows_df = dask_df[dask_df[column].isna() | dask_df[column].astype(str).eq("") ][["row_number"]]
            logger.info("Null or blank observation counts for " + column +  " : " + str(len(unexpected_rows_df)))
            unexpected_row_nums = unexpected_rows_df.compute()
            for row in unexpected_row_nums['row_number']:
                null_or_blank_check_alerts.append(
                    {
                        'row_number': row,
                        'COLUMN': column
                    }
                )
        quality_control_alerts["not_null_or_blank_check"] = null_or_blank_check_alerts
        logger.info("null_or_blank_check_alerts_count : " + str(len(quality_control_alerts["not_null_or_blank_check"])))
        logger.info(null_or_blank_check_alerts[0:5])
    except Exception as e:
        logger.error('Exception e : ' + str(e))