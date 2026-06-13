import pyspark.sql.functions as psf

def hash_aggregate(df, primary_key):

    non_key_cols = [c for c in sorted(df.columns) if c not in primary_key]
    
    # Use xxhash64 on each column natively without string casting
    row_hash = psf.xxhash64(*[psf.col(c) for c in non_key_cols])
    
    return df.withColumn("row_hash", row_hash) \
        .groupBy(primary_key) \
        .agg(psf.sum("row_hash").alias("group_hash"))


def generate_hash_delta(base_df, update_df, primary_key):

    base_hashes = hash_aggregate(base_df, primary_key)
    update_hashes = hash_aggregate(update_df, primary_key)
    joined_hashes = base_hashes.alias("base").join(update_hashes.alias("update"), on=primary_key, how="full_outer").persist()
    joined_hashes.count()

    changed = joined_hashes.filter(
        psf.col("base.group_hash").isNull() | 
        (psf.col("base.group_hash") != psf.col("update.group_hash"))
    ).select([psf.col(f"update.{k}").alias(k) for k in primary_key])

    delta_df = changed.join(update_df, on=primary_key, how="inner")

    joined_hashes.unpersist()
    return delta_df

def generate_subtract_delta(base_df, update_df, primary_key):

    added_or_changed = update_df.subtract(base_df).select(primary_key).distinct()

    existing_blocks = update_df.select(primary_key).distinct()

    removed_rows = base_df.subtract(update_df).select(primary_key).distinct().join(existing_blocks.hint("merge"), on=primary_key, how="inner")
    
    deltas = added_or_changed.union(removed_rows).distinct()

    all_deltas = deltas.join(update_df.hint("merge"), on=primary_key, how="inner")

    return all_deltas
    
def generate_removed_delta(base_df, update_df, primary_key):
    return base_df.join(update_df.select(primary_key).distinct(), on=primary_key, how="left_anti")
