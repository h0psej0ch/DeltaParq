from pyspark.sql import DataFrame

def apply_non_volatile_delta(base_df: DataFrame, delta_df: DataFrame, primary_key: list[str]) -> DataFrame:
    base_without_delta = base_df.join(delta_df.select(primary_key).distinct(), on=primary_key, how="left_anti")

    return base_without_delta.union(delta_df)
