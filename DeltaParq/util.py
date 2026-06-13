from pyspark.sql import SparkSession, DataFrame

def decompose(dataframe, schema_config: dict):
    volatile_cols = schema_config["volatiles"] + schema_config["primary_key"]
    volatile = dataframe.select(volatile_cols).dropDuplicates(volatile_cols)
    non_volatile_cols = [x for x in dataframe.columns if x not in schema_config["volatiles"]]
    non_volatile = dataframe.select(non_volatile_cols)
    return (volatile, non_volatile)
