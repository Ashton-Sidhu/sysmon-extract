import itertools
import numpy as np
import os
import click

from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
from openhunt import ossem
from pathlib import Path
from typing import Union


def _create_spark_session(master: str) -> SparkSession:
    """
    Helper function to create a spark session based on master string.

    For spark cluster, format: spark://HOST:PORT
    For mesos/yarn, format: mesos://HOST:PORT

    Parameters
    ----------
    master : str
        Location of your spark cluster
    """

    spark = SparkSession.builder \
        .master(master) \
        .appName("sysmon") \
        .getOrCreate()

    spark.sql("set spark.sqlcaseSensitive=True")

    return spark


def _get_rule_schema(df, event_column: str, *ids: str) -> list:
    """
    Identify the columns needed to extract data from sysmon.

    Parameters
    ----------
    df: Spark DataFrame
        Spark Dataframe

    event_column : str
        Column storing nested sysmon data

    ids : str
        Rule number

    Returns
    -------
    list
        Columns to extract from sysmon
    """

    # I wonder if there's a better way to do this
    def has_column(col):
        try:
            df[f"{event_column}{col}"]
            return True
        except AnalysisException:
            return False

    column_names = [
        filter(
            has_column,
            ossem.getEventFields(
                platform="windows",
                provider="sysmon",
                event=f"event-{rule}")) for rule in ids]

    return np.unique(list(itertools.chain.from_iterable(column_names)))


def _get_file_format(filename: str) -> str:
    """
    Get the format type of a file: csv, json, etc

    Parameters
    ----------
    filename : str
        Filename

    Returns
    -------
    str
        csv, json, etc
    """

    file_type = Path(filename).suffix[1:]

    return file_type


def extract(filename: str,
            event: Union[tuple, list],
            output: str,
            header=True,
            log_column="",
            event_column="",
            additional_columns=None,
            single_file=False,
            master="",
            spark_session=None,
            as_spark_frame=False,
            as_pandas_frame=False):
    """
    Extracts logs from Sysmon based on the event(s) specified.

    Can be returned as a file, Spark DataFrame or Pandas DataFrame.

    Parameters
    ----------
    filename : str
        Input file name.

    event : Union[tuple, list]
        List of event ids to extract

    output : str
        Output file

    header : bool
        For csv files, if the first row contains the headers.

    log_column : str
        Column detailed the log source

    event_column : str
        Column name that is holding nested sysmon data

    additional_columns : Union[tuple, list]
        Additional columns to extract from your data. Data in a nested column must have the parent col.
        Example: ["col1", "col2.nested_col.nested_nested_col"]

    single_file : bool
        True to output as a single file.
        WARNING: If you do not have enough memory to hold the data, this will fail.

    master : str
        Spark master, not used if spark_session is provided. by default local
        For a cluster, spark://HOST:PORT
        For a mesos/yarn cluster, mesos://HOST:PORT

    spark_session : pyspark.sql.SparkSession
        Existing spark session to use, by default None

    as_spark_frame : bool
        Return extracted logs as a Spark DF, by default False

    as_pandas_frame : bool
        Return extracted logs as a Pandas DF, by default False
    """

    if not spark_session:
        spark = _create_spark_session(master)

    file_format = _get_file_format(filename)
    filename = filename if "hdfs://" in filename else f"file:///{filename}"
    output = output if "hdfs://" in filename else f"file:///{output}"
    output_format = _get_file_format(output)
    event_column = event_column + "." if event_column else ""

    if file_format == "csv" and header:
        df = spark.read.format(file_format).option(
            "header", "true").load(filename)
    else:
        df = spark.read.format(file_format).option(
            "mode", "DROPMALFORMED").load(filename)

    df.registerTempTable("sysmon")

    cols = _get_rule_schema(df, event_column, *event)
    cols = [f"{event_column}{col}" for col in cols]

    # Probably a better way to do this
    where_log_column = f"{log_column} = 'Microsoft-Windows-Sysmon/Operational' AND"
    where_clause = f"WHERE {where_log_column} (event_id = {' OR event_id = '.join(event)})"
    select_clause = f"SELECT {','.join(list(additional_columns) + cols)}"

    sysmon_df = spark.sql(
        select_clause + ",event_id FROM sysmon " + where_clause)

    # Release DataFrame from memory
    spark.catalog.dropTempView("sysmon")

    if as_spark_frame:
        frame = sysmon_df
    elif as_pandas_frame:
        frame = sysmon_df.toPandas()
    else:
        frame = None

        if single_file:
            if output_format == "csv":
                sysmon_df.coalesce(1).write.format(output_format).option(
                    "header", "true").mode("overwrite").save(output)
            else:
                sysmon_df.coalesce(1).write.format(
                    output_format).mode("overwrite").save(output)
        else:
            if output_format == "csv":
                sysmon_df.write.format(output_format).option(
                    "header", "true").mode("overwrite").save(output)
            else:
                sysmon_df.write.format(output_format).mode(
                    "overwrite").save(output)

    # Stop SparkContext
    if not spark_session:
        spark.stop()

    return frame
