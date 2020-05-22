# Sysmon Extract

Sysmon Extract is a library to extract events from the sysmon log type based off the event id. They can be extracted as a file (any big data format) with support for HDFS or in memory as a Spark or Pandas DataFrame. As a note, this library works best with Spark as it leverages it for the ETL process.

The library comes with a library, cli and UI.

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
## Table of Contents

- [Usage](#usage)
  * [Command Line Interface](#command-line)
  * [UI](#ui)
  * [Package](#package)
- [Installation](#installation)
- [Feedback](#feedback)

## Usage

### Command Line

```
Usage: sysxtract [OPTIONS]

Options:

  -i, --input-file PATH
  -h, --header
  -e, --event TEXT
  -lc, --log-column TEXT
  -ec, --event-column TEXT       [default: ]
  -a, --additional-columns TEXT
  -o, --output-file TEXT         [default: /home/sidhu/sysmon-extract/sysmon-output.csv]
  -s, --single-file
  -m, --master TEXT              [default: local]
  -ui, --start-ui
  --help                         Show this message and exit.
```

`sysxtract -i /media/sidhu/Seagate/empire_apt3_2019-05-14223117.json -e 1 -e 2 -lc log_name -ec event_data -s -a host.name -o /home/sidhu/output.json`

Let's break it down.

*Input file:* -i /media/sidhu/Seagate/empire_apt3_2019-05-14223117.json

*Sysmon Events to extract:* -e 1 -e 2

*Column in the dataset that describes the log source (Sysmon, Microsoft Security, Microsoft Audit, etc.):* -lc log_name

*Column in the dataset that contains the nested sysmon data (often event_data):* -ec event_data

*Output as a single file:* -s

*Additional columns to extract:* -a host.name

*Output file name:* /home/sidhu/output.json

### UI

`sysextract -ui`

![Alt Text](docs/media/ui.gif)

### Package

Using the example above:

```python
from sysxtract import extract

# Extract to a file
extract(
    "/media/sidhu/Seagate/empire_apt3_2019-05-14223117.json",
    [1, 2],
    log_column="log_name",
    event_column="event_data",
    additional_columns="host.name",
    single_file=True,
    output_file="/home/sidhu/output.json"
)

# Extract to a file using an existing Spark cluster
extract(
    "/media/sidhu/Seagate/empire_apt3_2019-05-14223117.json",
    [1, 2],
    log_column="log_name",
    event_column="event_data",
    additional_columns="host.name",
    single_file=True,
    output_file="/home/sidhu/output.json",
    master="spark://HOST:PORT" # mesos://HOST:PORT for yarn/mesos cluster
)

# Extract to a file using an existing spark session
extract(
    "/media/sidhu/Seagate/empire_apt3_2019-05-14223117.json",
    [1, 2],
    log_column="log_name",
    event_column="event_data",
    additional_columns="host.name",
    single_file=True,
    output_file="/home/sidhu/output.json",
    spark_sess=spark, # spark session variable, usually named spark
)

# Extract to a Spark DataFrame
# NOTE: Must provide an existing Spark Session
extract(
    "/media/sidhu/Seagate/empire_apt3_2019-05-14223117.json",
    [1, 2],
    log_column="log_name",
    event_column="event_data",
    additional_columns="host.name",
    single_file=True,
    spark_sess=spark, # spark session variable, usually named spark
    as_spark_frame=True
)

# Extract to a Pandas DataFrame
df = extract(
    "/media/sidhu/Seagate/empire_apt3_2019-05-14223117.json",
    [1, 2],
    log_column="log_name",
    event_column="event_data",
    additional_columns="host.name",
    single_file=True,
    as_pandas_frame=True
)

# Extract using SparkDf as input
# NOTE: Must provide an existing Spark Session
df = extract(
    spark_df,
    [1, 2],
    log_column="log_name",
    event_column="event_data",
    additional_columns="host.name",
    single_file=True,
    as_pandas_frame=True
)

# Extract using PandasDf as input
# NOTE: To use a Pandas DataFrame as input and a Spark DataFrame as output, a Spark Session must be provided.
df = extract(
    pandas_df,
    [1, 2],
    log_column="log_name",
    event_column="event_data",
    additional_columns="host.name",
    single_file=True,
    as_pandas_frame=True
)
```

## Installation

`pip install sysxtract`

Since this library leverages Spark, specifically PySpark, you need to install it manually. This allows for version compatability when connecting to existing clusters.

`pip install pyspark==$VERSION`.

If you're going to use spark locally:

`pip install pyspark`

## Feedback

I appreciate any feedback so if you have any feature requests or issues make an issue with the appropriate tag or futhermore, send me an email at sidhuashton@gmail.com
