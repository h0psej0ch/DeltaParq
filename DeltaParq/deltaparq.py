from dataclasses import dataclass
from pyspark.sql import SparkSession, DataFrame, Column
from pyspark.storagelevel import StorageLevel
from calendar import monthrange
from DeltaParq.util import decompose
from DeltaParq.delta_decoder import apply_non_volatile_delta
from DeltaParq.strategies import get_strategy
from DeltaParq.dp_dataframe import DeltaParqDataFrame, DeltaParqDataFrameCollection
import DeltaParq.delta_generator as delta_generator
import warnings
from typing import Callable

        
@dataclass
class DeltaParq:
    spark: SparkSession
    config: dict

    def put(self, new_dataframe: DataFrame, tld: str, year: int, month: int, day: int):
        """Write a new day's data into the DeltaParq storage.

        Args:
            new_dataframe: The combined DataFrame to persist for the day.
            tld: Top-level domain or partition key used in path construction.
            year: Four-digit year for the partition.
            month: Month (1-12) for the partition.
            day: Day of month. If `day == 1` the method creates the month
                configuration and writes a full snapshot.

        Raises:
            Exception: If an unsupported delta generation type is configured
        """
        if day == 1:
            month_config = None
            schema = self.config["monthly_config"]["schema"]
        else:
            month_config = self._get_month_config(tld, year, month)
            schema = month_config["schema"]

        volatile_df, non_volatile_df = decompose(new_dataframe, schema)

        non_volatile_df = non_volatile_df.persist(StorageLevel.MEMORY_ONLY)

        if day == 1:
            self._create_month_config(tld, year, month)
            non_volatile_s3_string = self._get_path_string(tld, year, month, day=day, is_volatile=False)
            non_volatile_df.coalesce(self.config["storage"]["s3"]["partitions"]).write.mode("overwrite").option("compression", "gzip").parquet(non_volatile_s3_string)
        else:
            strat = get_strategy(month_config["strategy"])
            if strat(day) == day:
                # Full Snapshot
                non_volatile_s3_string = self._get_path_string(tld, year, month, day=day, is_volatile=False)
                non_volatile_df.coalesce(self.config["storage"]["s3"]["partitions"]).write.mode("overwrite").option("compression", "gzip").parquet(non_volatile_s3_string)
            else:
                # Delta
                primary_key = schema["primary_key"]

                base_df = self._retrieve_non_volatile_dataframes(tld, year, month, [strat(day)], month_config=month_config)[0]
                base_df.persist()
                delta_df = self._create_delta(base_df, non_volatile_df, self.config["delta_generation_type"], primary_key)
                delta_df.persist()

                if month_config["explicit_removal_save"]:
                    delta_s3_string = self._get_path_string(tld, year, month, day=day, is_volatile=False, removal=False)
                    delta_df.coalesce(self.config["storage"]["s3"]["partitions"]).write.mode("overwrite").option("compression", "gzip").parquet(delta_s3_string)
                    removed_delta_df = delta_generator.generate_removed_delta(base_df, non_volatile_df, primary_key)
                    removed_delta_s3_string = self._get_path_string(tld, year, month, day=day, is_volatile=False, removal=True)
                    removed_delta_df.coalesce(self.config["storage"]["s3"]["partitions"]).write.mode("overwrite").option("compression", "gzip").parquet(removed_delta_s3_string)
                else:
                    delta_s3_string = self._get_path_string(tld, year, month, day=day, is_volatile=False)
                    delta_df.coalesce(self.config["storage"]["s3"]["partitions"]).write.mode("overwrite").option("compression", "gzip").parquet(delta_s3_string)
                base_df.unpersist()
                delta_df.unpersist()

        volatile_s3_string = self._get_path_string(tld, year, month, day=day, is_volatile=True)
        volatile_df.coalesce(self.config["storage"]["s3"]["partitions"]).write.mode("overwrite").option("compression", "gzip").parquet(volatile_s3_string)
        
        # Clean up the persisted non_volatile_df now that all writes are complete
        non_volatile_df.unpersist()

    def put_list(self, dataframes: list[DataFrame], tld: str, year: int, month: int, days: list[int]):
        """Write a list of new dataframes into the DeltaParq storage.

        Args:
            dataframes: All the DataFrames to store in the DeltaParq storage.
            tld: Top-level domain or partition key used in path construction.
            year: Four-digit year for the partition.
            month: Month (1-12) for the partition.
            days: The list of all days to be stored. Each entry in the list should 
                cohere to the dataframe of the same index in the dataframes list.

        Raises:
            Exception: If an unsupported delta generation type is configured
        """
        
        dataframe_cache = {}


        month_config = self.config["monthly_config"] if 1 in days else self._get_month_config(tld, year, month)

        schema = month_config["schema"]
        strategy = get_strategy(month_config["strategy"])

        volatiles, non_volatiles = zip(*(list(map(lambda x: decompose(x, schema) , dataframes))))

        for i, df in enumerate(non_volatiles):
            day = days[i]
            dataframe_cache[str(day)] = df


        if 1 in days:
            self._create_month_config(tld, year, month)
            first_day_df = non_volatiles[days.index(1)]
            non_volatile_s3_string = self._get_path_string(tld, year, month, day=1, is_volatile=False)
            first_day_df.coalesce(self.config["storage"]["s3"]["partitions"]).write.mode("overwrite").option("compression", "gzip").parquet(non_volatile_s3_string)
            dataframe_cache["1"] = first_day_df


        to_resolve = []

        for day in days:
            if strategy(day) not in days:
                to_resolve.append(strategy(day))
        
        resolved_dataframes = self._retrieve_non_volatile_dataframes(tld, year, month, to_resolve, month_config)

        for i, df in enumerate(resolved_dataframes):
            dataframe_cache[str(to_resolve[i])] = df
        
        ordered_days = [x for x in self._resolve_dependencies(strategy, days) if x in days and x != 1]
        ordered_originals = [non_volatiles[days.index(day)] for day in ordered_days]

        for (day, update_df) in zip(ordered_days, ordered_originals):
            if strategy(day) == day:
                # Full Snapshot
                non_volatile_s3_string = self._get_path_string(tld, year, month, day=day, is_volatile=False)
                update_df.coalesce(self.config["storage"]["s3"]["partitions"]).write.mode("overwrite").option("compression", "gzip").parquet(non_volatile_s3_string)
            else:
                # Delta
                base_df = dataframe_cache[str(strategy(day))]

                primary_key = schema["primary_key"]
                delta_df = self._create_delta(base_df, update_df, self.config["delta_generation_type"], primary_key)
                if month_config["explicit_removal_save"]:
                    delta_s3_string = self._get_path_string(tld, year, month, day=day, is_volatile=False, removal=False)
                    delta_df.coalesce(self.config["storage"]["s3"]["partitions"]).write.mode("overwrite").option("compression", "gzip").parquet(delta_s3_string)
                    removed_delta_df = delta_generator.generate_removed_delta(base_df, update_df, primary_key)
                    remove_delta_s3_string = self._get_path_string(tld, year, month, day=day, is_volatile=False, removal=True)
                    removed_delta_df.coalesce(self.config["storage"]["s3"]["partitions"]).write.mode("overwrite").option("compression", "gzip").parquet(remove_delta_s3_string)
                else:
                    delta_s3_string = self._get_path_string(tld, year, month, day=day, is_volatile=False)
                    delta_df.coalesce(self.config["storage"]["s3"]["partitions"]).write.mode("overwrite").option("compression", "gzip").parquet(delta_s3_string)

        for i, df in enumerate(volatiles):
            day = days[i]
            volatile_s3_string = self._get_path_string(tld, year, month, day=day, is_volatile=True)
            df.coalesce(self.config["storage"]["s3"]["partitions"]).write.mode("overwrite").option("compression", "gzip").parquet(volatile_s3_string)

        return

    def get(self, tld: str, year: int, month: int, day: int, use_volatile: bool = True, column_selection: list[str] = ['*'], filter: Column | str = None) -> DataFrame:
        """Return the reconstructed DataFrame for a single day.

        Args:
            tld: Partition key used in storage paths.
            year: Year partition.
            month: Month partition.
            day: Day to retrieve.
            use_volatile: If False and supported by the month config, the
                returned DataFrame will be derived solely from non-volatile
                data (avoiding the volatile join).

        Returns:
            A Spark `DataFrame` representing the day's table.
        """
        return self._retrieve_dataframe(tld, year, month, day, use_volatile, column_selection=column_selection, filter=filter)
    
    def get_range(self, tld: str, year: int, month: int, start_day: int, end_day: int, use_volatile: bool = True, column_selection: list[str] = ['*'], filter: Column | str = None) -> list[DataFrame]:
        """Retrieve a list of daily DataFrames for a contiguous day range.

        Args:
            tld: Partition key used in storage paths.
            year: Year partition.
            month: Month partition.
            start_day: Start of the day range (inclusive).
            end_day: End of the day range (exclusive).
            use_volatile: If False and supported by the month config, the
                returned DataFrame will be derived solely from non-volatile
                data (avoiding the volatile join).

        Returns:
            A list of Spark `DataFrame` objects for each day in the range.
        """
        return self._retrieve_dataframes(tld, year, month, list(range(start_day, end_day)), use_volatile, column_selection=column_selection, filter=filter)
    
    def rebuild(self, tld: str, year: int, month: int, output_path: str):
        """Retrieve a the original data of daily DataFrames for a full month.

        Args:
            tld: Partition key used in storage paths.
            year: Year partition.
            month: Month partition.
            output_path: The (S3) path to where the reconstructed dataframes will be stored.
        """

        month_config = self._get_month_config(tld, year, month)
        strategy = get_strategy(month_config["strategy"])
        _, days = monthrange(year, month)
        days_list = list(range(1,days+1))
        dependency_chain = self._resolve_dependencies(strategy, days_list)
        cache = dict()
        for day in dependency_chain:
            if strategy(day) == day:
                non_volatile = self._retrieve_snapshot(tld, year, month, day, ["*"], None)
                cache[str(day)] = non_volatile
                joined = self._join_non_volatile_with_volatile(tld, year, month, day, non_volatile, month_config["schema"]["primary_key"], ["*"])
                joined.coalesce(self.config["storage"]["s3"]["partitions"]).write.option("compression", "gzip").mode("overwrite").parquet(f"{output_path}/day={day:02}")
            else:
                base_df = cache[str(strategy(day))]
                non_volatile = self._retrieve_and_apply_delta(tld, year, month, day, base_df, None, month_config["schema"]["primary_key"], ["*"], None)
                cache[str(day)] = non_volatile
                joined = self._join_non_volatile_with_volatile(tld, year, month, day, non_volatile, month_config["schema"]["primary_key"], ["*"])
                joined.coalesce(self.config["storage"]["s3"]["partitions"]).write.option("compression", "gzip").mode("overwrite").parquet(f"{output_path}/day={day:02}")
        return

    def _get_path_string(self, tld: str, year: int, month: int, day: int = 0, is_volatile: bool = False, removal: bool = None) -> str:
        """Construct an S3 path string for the given partition parameters.

        Args:
            tld: Partition key used as a top-level directory.
            year: Year integer.
            month: Month integer.
            day: Optional day integer; when zero only the month-level path is
                returned.
            is_volatile: Whether this path points to volatile data.
            removal: If provided, appends a removal marker to the path.
                Can be used for storage where removals are explicitly saved.

        Returns:
            A string with the S3 path using `s3a://` scheme.
        """
        bucket = self.config["storage"]["s3"]["bucket"]
        date_path = f"/year={year:04}/month={month:02}" + (f"/day={day:02}/volatile={is_volatile}" if day != 0 else "")
        removal_string = f"/removal={removal}" if removal != None else ""
        return f"s3a://{bucket}/tld={tld}{date_path}{removal_string}" 

    def _create_month_config(self, tld: str, year: int, month: int):
        """Write the month-level configuration from the configuration file to JSON in storage.

        Args:
            tld: Partition key.
            year: Year partition.
            month: Month partition.
        """
        path = self._get_path_string(tld, year, month) + "/_config.json"
        self.spark.createDataFrame([self.config["monthly_config"]]).coalesce(1).write.mode("errorifexists").json(path)
    
    def _get_month_config(self, tld: str, year: int, month: int) -> dict:
        """Load the month-level configuration from storage.

        Args:
            tld: Partition key.
            year: Year partition.
            month: Month partition.

        Returns:
            A Python `dict` representing the stored month configuration.
        """
        path = self._get_path_string(tld, year, month) + "/_config.json"
        return self.spark.read.format("json").load(path).first().asDict()
    
    def _retrieve_dataframe(self, tld: str, year: int, month: int, day: int, use_volatile: bool, column_selection: list[str] = ['*'], filter: Column | str = None) -> DataFrame:
        """Retrieve a single day's DataFrame (helper wrapper for _retrieve_dataframes).
        """
        return self._retrieve_dataframes(tld, year, month, [day], use_volatile, column_selection=column_selection, filter=filter)[0]
    
    def _retrieve_dataframes(self, tld: str, year: int, month: int, days: list[int], use_volatile: bool, column_selection: list[str] = ['*'], filter: Column | str = None) -> list[DataFrame]:
        """Retrieve reconstructed DataFrames for a list of days.

        For each requested day this will load the appropriate non-volatile
        snapshot or apply deltas and (by default) join the volatile data to
        return the full table for that day.

        Args:
            tld: Partition key.
            year: Year partition.
            month: Month partition.
            days: List of day integers to retrieve.
            use_volatile: If False and supported by the month config, the
                returned DataFrame will be derived solely from non-volatile
                data (avoiding the volatile join). 

        Returns:
            A list of `DeltaParqDataFrame` objects corresponding to `days`.
        """
        month_config = self._get_month_config(tld, year, month)
        primary_key = month_config["schema"]["primary_key"]
        non_volatile_column_selection = ['*'] if '*' in column_selection else [c for c in column_selection if c not in month_config["schema"]["volatiles"]]
        if non_volatile_column_selection != ['*']:
            non_volatile_column_selection.extend(k for k in primary_key if k not in non_volatile_column_selection)
        volatile_column_selection = ['*'] if '*' in column_selection else [c for c in column_selection if c in month_config["schema"]["volatiles"]]
        if volatile_column_selection != ['*']:
            volatile_column_selection.extend(k for k in primary_key if k not in volatile_column_selection)
        non_volatile_dataframes = self._retrieve_non_volatile_dataframes(tld, year, month, days, month_config=month_config, column_selection=non_volatile_column_selection, filter=filter)
        if not use_volatile:
            if month_config["explicit_removal_save"]:
                return [(self._apply_removal_delta(tld, year, month, day, df, primary_key) if get_strategy(month_config["strategy"])(day) != day else df) for (day, df) in zip(days, non_volatile_dataframes)]
            else:
                warnings.warn(f"The stored data for {year}-{month:02} of tld {tld}, does not support not using optimization by not using the volatile data. A full table will be generated instead")
        return [self._join_non_volatile_with_volatile(tld, year, month, day, df, primary_key, column_selection=volatile_column_selection) for (day, df) in zip(days, non_volatile_dataframes)]
    
    def _retrieve_non_volatile_dataframes(self, tld: str, year: int, month: int, days: list[int], month_config: dict, column_selection: list[str] = ['*'], filter: Column | str = None) -> list[DataFrame]:
        """Resolve and return non-volatile DataFrames for requested days.

        The method computes the dependency chain induced by the storage
        strategy and either loads snapshots or applies deltas to reconstruct
        the requested day's non-volatile state.

        Args:
            tld: Partition key.
            year: Year partition.
            month: Month partition.
            days: List of day integers to retrieve.
            month_config: Month-level configuration dict.

        Returns:
            A list of `DeltaParqDataFrame` objects containing the non-volatile
            state for each requested day.
        """
        strategy = get_strategy(month_config["strategy"])
        dependency_chain = self._resolve_dependencies(strategy, days)
        resolved = {}
        for day in dependency_chain:
            if strategy(day) == day:
                resolved[str(day)] = self._retrieve_snapshot(tld, year, month, day, column_selection, filter)
            else:
                parent = strategy(day)
                primary_key = month_config["schema"]["primary_key"]
                parent_df = resolved[str(parent)]
                applied_delta = self._retrieve_and_apply_delta(tld, year, month, day, parent_df, False if month_config["explicit_removal_save"] else None, primary_key, column_selection, filter)
                applied_delta = applied_delta.localCheckpoint() # force evaluation
                resolved[str(day)] = applied_delta 
                    
        return [resolved[str(d)] for d in days]

    def _retrieve_snapshot(self, tld: str, year: int, month: int, day: int, column_selection: list[str], filter: Column | str) -> DataFrame:
        """Load a full non-volatile snapshot for a specific day.

        Args:
            tld: Partition key.
            year: Year partition.
            month: Month partition.
            day: Day integer whose snapshot should be loaded.

        Returns:
            A Spark `DataFrame` read directly from the parquet files.
        """
        non_volatile_s3_path = self._get_path_string(tld, year, month, day=day, is_volatile=False)
        selected_df = self.spark.read.parquet(non_volatile_s3_path).select(column_selection)
        return selected_df.filter(filter) if filter is not None else selected_df

    def _retrieve_and_apply_delta(self, tld: str, year: int, month: int, day: int, base_df: DataFrame, explicit_removal_save: bool, primary_key: list[str], column_selection: list[str], filter: Column | str) -> DataFrame:
        """Retrieve the delta for `day` and apply it to `base_df`.

        Args:
            tld: Partition key.
            year: Year partition.
            month: Month partition.
            day: Day whose delta should be applied.
            base_df: The DataFrame representing the base snapshot to fold the delta onto.
            explicit_removal_save: If False/True this selects a removal-marked
                delta path. If `None` the default non-removal path is used.
            primary_key: List of column names composing the primary key.

        Returns:
            A new Spark `DataFrame` with the delta applied.
        """

        delta_s3_path = self._get_path_string(tld, year, month, day=day, is_volatile=False, removal=explicit_removal_save)
        delta_df = self.spark.read.parquet(delta_s3_path).select(column_selection)
        delta_df = delta_df.filter(filter) if filter is not None else delta_df

        return apply_non_volatile_delta(base_df, delta_df, primary_key)

    def _join_non_volatile_with_volatile(self, tld: str, year: int, month: int, day: int, non_volatile_df: DataFrame, primary_key: list[str], column_selection: list[str]) -> DataFrame:
        """Join the non-volatile DataFrame with the volatile layer for a day.


        Args: 
            tld: Partition key.
            year: Year partition.
            month: Month partition.
            day: Day whose delta should be applied.
            non_volatile_df: The DataFrame containing all the non volatile data for the day.
            primary_key: List of column names composing the primary key.

        Returns:
            A spark `DataFrame` with the volatile and non volatile data joined together.
        """
        volatile_s3_path = self._get_path_string(tld, year, month, day=day, is_volatile=True)
        volatile_df = self.spark.read.parquet(volatile_s3_path).select(column_selection)
        
        return non_volatile_df.join(volatile_df, on=primary_key, how="inner")

    def _apply_removal_delta(self, tld: str, year: int, month: int, day: int, non_volatile_df: DataFrame, primary_key: list[str]) -> DataFrame:
        """Apply a removal delta (delete marker) to the non-volatile DataFrame.

        Args: 
            tld: Partition key.
            year: Year partition.
            month: Month partition.
            day: Day whose delta should be applied.
            non_volatile_df: The DataFrame containing all the non volatile data for the day.
            primary_key: List of column names composing the primary key.

        Returns:
            A spark `DataFrame` with the supplied non volatile data with all rows marked in the removal table excluded.
        """
        removal_delta_s3_path = self._get_path_string(tld, year, month, day=day, is_volatile=False, removal=True)
        removal_delta_df = self.spark.read.parquet(removal_delta_s3_path)

        return non_volatile_df.join(removal_delta_df, on=primary_key, how="left_anti")

    def _resolve_dependencies(self, strategy: Callable[[int], int], days: list[int]) -> list[int]:
        """Resolve the dependency chain for a list of days, sorted such that a day can always be constructed with the ones preceding in the list.

        Args:
            strategy: A callable function that maps a day to the day it depends on.
            days: List of day integers to resolve dependencies for.

        Returns:
            A list of day integers in the order they should be processed.
        """
        dependencies = []
        for day in days:
            to_add = []
            while True:
                if day in dependencies:
                    index_of_day = dependencies.index(day)
                    dependencies = dependencies[0:index_of_day] + to_add + dependencies[index_of_day:]
                    break
                else:
                    to_add.append(day)
                    if strategy(day) == day:
                        dependencies = dependencies + to_add
                        break
                    day = strategy(day)
        return list(reversed(dependencies))
                    
    def _create_delta(self, base_df: DataFrame, update_df: DataFrame, delta_generation_type: str, primary_key: list[str]) -> DataFrame:
        """Create a delta DataFrame from `base_df` and `update_df`.

        Args:
            base_df: The base DataFrame representing the previous state.
            update_df: The new state DataFrame to compare against `base_df`.
            delta_generation_type: One of the supported strategies such as
                `'subtract'` or `'hash'`.
            primary_key: List of column names used as the primary key.

        Returns:
            A Spark `DataFrame` containing the computed delta.

        Raises:
            Exception: If `delta_generation_type` is not supported.
        """
        match delta_generation_type:
            case "subtract": return delta_generator.generate_subtract_delta(base_df, update_df, primary_key)
            case "hash": return delta_generator.generate_hash_delta(base_df, update_df, primary_key)
            case _: raise Exception(f"Delta Generation Type \"{delta_generation_type}\" not available")
