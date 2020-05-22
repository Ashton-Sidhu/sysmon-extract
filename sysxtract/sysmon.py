import itertools
from pathlib import Path
from typing import Union

import numpy as np
import pandas as pd

from openhunt import ossem
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.utils import AnalysisException


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


def _get_rule_schema(df: DataFrame, event_column: str, *ids: str) -> list:
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


def _get_df(spark: SparkSession, input_data: Union[str, pd.DataFrame, DataFrame], header: bool) -> DataFrame:
    """
    Returns a Spark Dataframe based on input.

    str -> reads file
    Pandas DataFrame -> converts it to SparkDf
    SparkDf -> Do nothing.

    Parameters
    ----------
    spark : SparkSession
        Spark session

    input_data : Union[str, pd.DataFrame, DataFrame]
        Filename, Pandas DF, Spark DF

    header: bool
        For csvs, whether the first row contains column names.

    Returns
    -------
    DataFrame
        Spark DF
    """

    if isinstance(input_data, str):
        file_format = _get_file_format(input_data)
        filename = filename if "hdfs://" in filename else f"file:///{filename}"

        if file_format == "csv" and header:
            df = spark.read.format(file_format).option(
                "header", "true").load(filename)
        else:
            df = spark.read.format(file_format).option(
                "mode", "DROPMALFORMED").load(filename)

    elif isinstance(input_data, pd.DataFrame):
        df = spark.createDataFrame(input_data)

    else:
        df = input_data


def _return_(as_spark_frame: bool, sysmon_df: DataFrame, as_pandas_frame: bool, single_file: bool, output_format: str, output_file: str):
    """
    Outputs the result as either a file or a frame.

    Parameters
    ----------
    as_spark_frame : bool
        Return a SparkDf

    sysmon_df : bool
        DataFrame

    as_pandas_frame : bool
        Return a PandasDf

    single_file : bool
        Return a single file

    output_format : str
        Output file format

    output_file : str
        Output file path

    Returns
    -------
    pd.DataFrame, DataFrame
        Nothing if write to file, Pandas Dataframe or Spark DataFrame
    """

    if as_spark_frame:
        frame = sysmon_df
    elif as_pandas_frame:
        frame = sysmon_df.toPandas()
    else:
        frame = None

        if single_file:
            if output_format == "csv":
                sysmon_df.coalesce(1).write.format(output_format).option(
                    "header", "true").mode("overwrite").save(output_file)
            else:
                sysmon_df.coalesce(1).write.format(
                    output_format).mode("overwrite").save(output_file)
        else:
            if output_format == "csv":
                sysmon_df.write.format(output_format).option(
                    "header", "true").mode("overwrite").save(output_file)
            else:
                sysmon_df.write.format(output_format).mode(
                    "overwrite").save(output_file)
    return frame


def extract(input_data: Union[str, pd.DataFrame, DataFrame],
            event: Union[tuple, list],
            output_file="",
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
    input_data : str, pd.DataFrame, DataFrame
        Input file name or frames.

    event : Union[tuple, list]
        List of event ids to extract

    output_file : str
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

    assert (output_file or as_spark_frame or as_pandas_frame), "Please provide an output format (output_file, as_spark_frame, as_pandas_frame."

    if as_spark_frame and not spark_session:
        raise ValueError(
            "To return a Spark DataFrame, an existing SparkSession must be provided. Please provide a SparkSession or output as a Pandas DataFrame or a file.")

    spark = spark_session if spark_session else _create_spark_session(master)
    df = _get_df(spark, input_data, header)
    event_column = event_column + "." if event_column else ""

    if output_file:
        output_format = _get_file_format(output_file)
        output_file = output_file if "hdfs://" in output_file else f"file:///{output_file}"

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

    frame = _return_(
        as_spark_frame,
        sysmon_df,
        as_pandas_frame,
        single_file,
        output_format,
        output_file
    )

    # Stop SparkContext
    if not spark_session:
        spark.stop()

    return frame
