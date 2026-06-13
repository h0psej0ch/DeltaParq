import pyspark.sql.functions as psf
from pyspark.storagelevel import StorageLevel

def hash_aggregate(df, primary_key: list[str]):
    value_cols = [x for x in df.columns if x not in primary_key]
    # Combine all value columns into a single hash per row
    row_hash = psf.md5(psf.concat_ws("||", *[psf.col(c).cast("string") 
                                          for c in value_cols]))
    
    # Aggregate all row hashes within a block into one block hash
    # order-insensitively by sorting before aggregating
    block_hash = (
        df.withColumn("_row_hash", row_hash)
          .groupBy(*primary_key)
          .agg(
              psf.sort_array(psf.collect_list("_row_hash")).alias("_sorted_hashes")
          )
          .withColumn("_block_hash", psf.md5(psf.concat_ws(",", "_sorted_hashes")))
          .drop("_sorted_hashes")
    )
    return block_hash

def generate_hash_delta(base_df, update_df, primary_key):
    base_hashes = hash_aggregate(base_df, primary_key)
    update_df.persist(StorageLevel.MEMORY_ONLY)
    update_hashes = hash_aggregate(update_df, primary_key)

    joined = update_hashes.alias("updated").join(
        base_hashes.alias("base"),
        on=primary_key,
        how="left"
    )

    changed_keys = joined.filter(
        psf.col("base._block_hash").isNull() |
        (psf.col("base._block_hash") != psf.col("updated._block_hash"))
    ).select(*[psf.col(f"updated.{key}") for key in primary_key])

    returnable = update_df.join(
        psf.broadcast(changed_keys),
        on=primary_key,
        how="right"
    ) 
    update_df.unpersist()
    return returnable

def generate_subtract_delta(base_df, update_df, primary_key):

    added_or_changed = update_df.subtract(base_df).select(primary_key).distinct()

    existing_blocks = update_df.select(primary_key).distinct()

    removed_rows = base_df.subtract(update_df).select(primary_key).distinct().join(existing_blocks.hint("merge"), on=primary_key, how="inner")
    
    deltas = added_or_changed.union(removed_rows).distinct()

    all_deltas = deltas.join(update_df.hint("merge"), on=primary_key, how="inner")

    return all_deltas
    
def generate_removed_delta(base_df, update_df, primary_key):
    return base_df.join(update_df.select(primary_key).distinct(), on=primary_key, how="left_anti")
