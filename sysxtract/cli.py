import click
import os

from sysxtract.sysmon import extract
from sysxtract.ui import start_ui


@click.command()
@click.option("-i", "--input-file", type=click.Path(exists=True),)
@click.option("-h", "--header", is_flag=True)
@click.option("-e", "--event", multiple=True,)
@click.option("-lc", "--log-column", default="")
@click.option("-ec", "--event-column", show_default=True)
@click.option("-a", "--additional-columns", default=[], multiple=True)
@click.option("-o", "--output-file",
              default=f"{os.getcwd()}/sysmon-output.csv", show_default=True)
@click.option("-s", "--single-file", is_flag=True)
@click.option("-m", "--master", default="local", show_default=True)
@click.option("-ui", "--start-ui", is_flag=True)
def cli(input_file, header, event, log_column, event_column,
        additional_columns, output_file, single_file, master, start_ui):
    """
    Sysmon-extract extracts sysmon logs based off their event ids into various file formats.

    INPUT_FILE is the input file of all logs. Must be the full path. Supports HDFS.

    HEADER is specifically for csvs, if the first line is column names

    EVENT is the event id you want to extract

    LOG_COLUMN is if you have multiple log types in one file, sysmon, security, application logs etc. Specify the column name that describes the log source (i.e. log_name, channel, etc.)
    
    EVENT_COLUMN is if your sysmon data is nested into another column, specify the column name that has the nexted sysmon data (i.e. event_data)
    
    ADDITIONAL_COLUMNS is if you want any additional columns from your dataset that are not in the event columns
    
    OUTPUT_FILE is the output file for the new dataset. Must be the full path. Supports HDFS.
    
    SINGLE FILE is if you want the output data as a single file. WARNING: You must have memory large enough to support this.
    
    MASTER is the master url for your spark instance

    START_UI starts the UI on port 8501
    """

    if start_ui:
        path = f"{os.path.dirname(__file__)}/ui.py"
        os.system(f"streamlit run {path}")
    else:
        if not input_file: 
            click.echo("Please provide an input file (-i)")
            return

        if not event:
            click.echo("Please provide events to extract (-e)")
            return

        extract(
            input_file,
            event,
            output_file=output_file,
            header=header,
            log_column=log_column,
            event_column=event_column,
            additional_columns=additional_columns,
            single_file=single_file,
            master=master
        )
