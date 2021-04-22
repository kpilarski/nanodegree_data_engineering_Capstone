from pyspark.sql.functions import *

def quality_check_rows(spark, path_parquet, table_name):
    """
    Function for first quality check.
    It validates if given table has any rows -
    If table has any rows it means that ETL process worked fine
    and the rows are in the table - Then the check is passed.
    """   
    
    df = spark.read.parquet(path_parquet)
    rows = df.count()
    
    if rows == 0:
        print(f" - Table {table_name} has 0 rows - quality check failed")
    else:
        print(f" - Table {table_name} has {rows} rows - quality check passed")
    return None


def quality_check_joins(spark, path_fact, path_dim_to_check, fact_col, dim_col):
    """
    Function for second quality check.
    It validates if join between fact and dimension table is working correctly -
    for verification -left_anti- is used here.
    """    
    
    df = spark.read.parquet(path_fact)
    df_to_check = spark.read.parquet(path_dim_to_check)
    
    common_values = df.select(col(fact_col)).distinct() \
                             .join(df_to_check, df[fact_col] == df_to_check[dim_col], "left_anti") \
                             .count()
    
    if common_values == 0:
        print(f" - Quality check passed - join between those two tables works correctly")
    else:
        print(f" - Quality check failed - join between those two tables works incorrectly")
    return None