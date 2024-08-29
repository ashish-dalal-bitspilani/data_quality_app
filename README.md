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
    - Justification : Although, we do have a monotonically increasing EMP_ID from 1 to 1000000 within the dataset, it is standard practice to have row numbers assigned to each row to make the code generic and reusable for cases where EMP_ID might be random, unique and might not follow any strict ordering structure
    - For pyspark, this was achieved through Window() utility 
    - For python(pandas), this was obtained by simply adding a new column having range from 1 to length of the dataframe
    - For dask, it was reset of index from 1 to the length of the dataframe
+ Getting a great expectations dataframe for corresponding Spark and Pandas dataframes (This step was skipped for modules without GE)
+ Applying standard expectations on dataframes to get an expectation outcome (This step was skipped for modules without GE)
  - Pyspark : Here the expectation outcome did not have unexpected index list due to the distributed nature of Spark, therefore, custom filtering of dataframe for fetching row numbers had to be added on top of GE standard expectation
  - Pandas : Here, the unexpected index list was sufficient to locate the row numbers for composing the required result format
+ For modules minus of GE, custom filtering logic based on filter, string equivalence, null checks using na functions has been applied

+ Time Complexity and Considerations :

Assuming dataset has n rows , m columns ((1000000,7)in this case) and w worker nodes

| Approach	| Time Complexity	|
|-----------|-------------------|
| Pyspark with GE| O((n*m)/w)|
| Pandas with GE| O(n*m)|
| Pyspark without GE| O((n*m)/w)|
| Pandas without GE| O((n*m)/w)|
| Dask without GE| O((n*m)/w)|

+ Considerations for above time complexities:
    - When comparing the time complexity of using PySpark vs pandas vs Dask to perform DQ checks on a column level for all columns of a DataFrame, there are key differences due to how PySpark/pandas/dask operate under the hood:
    - Pandas operates on in-memory data, meaning it loads the entire dataset into RAM. This allows for faster access and operations on smaller datasets but can become slow or infeasible as the dataset size increases due to memory constraints.
	- PySpark is designed for distributed processing. It processes data across multiple nodes in a cluster, making it more suitable for large-scale datasets that do not fit into memory.
	- However, due to PySpark's distributed nature, the actual runtime depends on the parallelization across the cluster. If the data is evenly distributed and the cluster is well-utilized, the effective time complexity can be reduced depending on the number of workers
	- Pandas with Great Expectations does not have distributed computation capabilities, so all operations occur in a single process.
	- PySpark introduces overhead in terms of job scheduling, network communication, and data shuffling, which can impact performance for smaller datasets. For very large datasets, these overheads are outweighed by the benefits of parallel processing.
	- The primary difference without Great Expectations is that both pandas and PySpark will only involve the data processing frameworks' native performance characteristics, without any additional overhead from the validation and assertion processes that Great Expectations introduces.
	- Dask has overhead related to task scheduling, data shuffling, and inter-worker communication, but it is often lighter than PySpark’s overhead due to Dask's more dynamic and flexible scheduler.
	- Dask is more efficient than pandas for datasets that don't fit in memory and often more lightweight compared to PySpark for many tasks.
	- The Great Expectations overhead is usually on the validation and assertion side, which is less significant in terms of order of complexity than the time complexity involved in the use of main libraries to perform the DQ checks