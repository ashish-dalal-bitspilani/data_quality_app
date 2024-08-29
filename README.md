# data_quality_app

Data Quality App performing quality checks for nullity, blanks, specific value(s) and character(s) using pyspark, pandas, dask and great expectations.

The overall app is composed of five modules attempting data quality checks:

+ Pyspark with great expectations
+ Pyspark without great expectations
+ Python (traditional pandas based approach) with great expectations
+ Python (traditional pandas based approach) without great expectations
+ Dask without great expectations

Dask with great expectations has not been explored as great expectations does not natively support dask dataframes yet. This feature has been oft requested by the data engineer dev community and the great expectations dev team is yet to roll out this native support.

As for the quality checks and expectations used, below checks were in-scope:

+ Special characters check - expect_column_values_to_not_match_regex
+ Country check - expect_column_values_to_be_in_set, expect_column_values_to_not_be_null
+ State check - expect_column_values_to_be_in_set, expect_column_values_to_not_be_null
+ Null/blank check - expect_column_values_to_not_be_null, expect_column_value_lengths_to_be_between

Approach Steps :

+ Getting a dataframe with row_numbers 
  * Justification : Although, we do have a monotonically increasing EMP_ID from 1 to 1000000 within the dataset, it is standard practice to have row numbers assigned to each row to make the code generic and reusable for cases where EMP_ID might be random, unique and might not follow any strict ordering structure
    - For pyspark, this was achieved through Window() utility 
    - For python(pandas), this was obtained by simply adding a new column having range from 1 to length of the dataframe
    - For dask, it was reset of index from 1 to the length of the dataframe
+ Getting a great expectations dataframe for corresponding Spark and Pandas dataframes (This step was skipped for modules without GE)
+ Applying standard expectations on dataframes to get an expectation outcome (This step was skipped for modules without GE)
  * Pyspark : Here the expectation outcome did not have unexpected index list due to the distributed nature of Spark, therefore, custom filtering of dataframe for fetching row numbers had to be added on top of GE standard expectation
  * Pandas : Here, the unexpected index list was sufficient to locate the row numbers for composing the required result format
+ For modules minus of GE, custom filtering logic based on filter, string equivalence, null checks using na functions has been applied

+ Time Complexity and Considerations :
Assuming dataset has n rows (1,000,000), m (in this case 7) columns and w worker nodes
|Approach	|Time Complexity	|
|:---   	|:--- 				|
|Pyspark with GE|O((n*m)/w)|
|Pandas with GE|O(n*m)|
|Pyspark without GE|O((n*m)/w)|
|Pandas without GE|O((n*m)/w)|
|Dask without GE|O((n*m)/w)|

