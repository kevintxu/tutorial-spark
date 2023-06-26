# %%
import pyspark
import logging
import s3fs
import sys
import argparse
import dateutil
import json
import pytz
import datetime

# %%
logger = logging.getLogger(__name__)

# %%
def create_df_from_script (spark: pyspark.sql.SparkSession, script_text: str, **params) -> pyspark.sql.DataFrame:
    sql_text = script_text.format( **params )
    logger.info ("Executing SQL:\n %s", sql_text)
    try:
        return_df = spark.sql(sql_text) 
    except:
        logger.error("Error executing SQL:\n %s", sql_text)
        raise 
    else:
        logger.info("Successfully executed SQL script.")
        return return_df


# %%
def load_df_from_file (spark: pyspark.sql.SparkSession, path_prefix: str, format: str, read_options: dict={}) -> pyspark.sql.DataFrame:
    logger.info ("Loading %s files in: %s\nWith options: %s", format, path_prefix, read_options)
    try:
        return_df = spark.read\
            .format(format)\
            .options(**read_options)\
            .load(path_prefix)
    except:
        logger.error("Error loading files from: %s", path_prefix)
        raise 
    else:
        logger.info("Successfully loaded file into data frame.")
        return return_df

# %%
def load_script (path: str) -> str:
    logger.info("Loading script file from: %s", path)
    match path[:5]:
        case 's3://':
            logger.info("Reading using S3 file handle.")
            fs = s3fs.S3FileSystem(anon=False)
            with fs.open(path[5:], 'r') as f:
                script_content = f.read()
        case _:
            logger.info("Reading using OS file handle.")
            with open(path, "r") as f:
                script_content = f.read()
    # if path[:5] == ['s3://']:
    #     fs = s3fs.S3FileSystem(anon=False)
    #     with fs.open(path[5:], 'r') as f:
    #         script_content = f.read()
    # else:
    #     with open(path, "r") as f:
    #         script_content = f.read()
    return script_content

# %%
def write_df_to_file (spark: pyspark.sql.SparkSession, df: pyspark.sql.DataFrame, format: str, path_prefix: str, write_options: dict={}):
    logger.info ("Writing %s files to: %s\nWith options: %s", format, path_prefix, write_options)
    try:
        df.write\
            .format(format)\
            .options(**write_options)\
            .save(path_prefix)
    except:
        logger.error("Error writing files to: %s", path_prefix)
        raise 
    else:
        logger.info("Successfully written Data Frame to %s file.", format)

# %%
def convert_file (spark: pyspark.sql.SparkSession, 
    source_prefix: str,
    dest_prefix: str,
    source_format: str,
    dest_format: str,
    script_text: str=None,
    read_options: dict={},
    write_options: dict={},
    temp_table_name: str="temp_table",
    **script_params):

    source_df = load_df_from_file (spark=spark, path_prefix=source_prefix, format=source_format, read_options=read_options)

    if script_text is None:
        output_df = source_df
    else:
        source_df.createOrReplaceTempView(temp_table_name)
        output_df = create_df_from_script(spark=spark, script_text=script_text, **script_params)

    write_df_to_file (spark=spark, df=output_df, format=dest_format, path_prefix=dest_prefix, write_options=write_options)

# %%
def parse_arguments(args):
    arg_parser = argparse.ArgumentParser(description = 'PySpark File Converter')
    arg_parser._action_groups.pop()
    required_args = arg_parser.add_argument_group('Required Arguments')
    optional_args = arg_parser.add_argument_group('Optional Arguments')
    required_args.add_argument("-i","--input_path", required=True, help="S3 Path / Directory prefix to read inputs.")
    required_args.add_argument("-o","--output_path", required=True, help="S3 Path / Directory prefix to store outputs.")
    required_args.add_argument("-if","--input_format", required=True, choices=['parquet', 'csv', 'json', 'orc', 'text', 'jdbc'],
        help="Format of the input files.")
    required_args.add_argument("-of","--output_format", required=True, choices=['parquet', 'csv', 'json', 'orc', 'text', 'jdbc'],
        help="Format of the output files.")

    optional_args.add_argument("-d", '--run_date', type=lambda x: dateutil.parser.parse(x), 
        help="Date to process")
    optional_args.add_argument("-s", '--script_path', help="Path to SQL transform script, if any.")
    optional_args.add_argument("-io", '--input_options', default={}, type=lambda x: json.loads(x), 
        help='Read options to be used by spark, in json format (eg. {"header": "true", "sep": "|"} for csv files or {"multiLine":"true"} for json files). Refer to: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.options.html#pyspark.sql.DataFrameWriter.options')
    optional_args.add_argument("-oo", '--output_options', default={}, type=lambda x: json.loads(x), 
        help='Write options to be used by spark, in json format (eg. {"header": "true", "sep": "|"} for csv files or {"multiLine":"true"} for json files). Refer to: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.options.html#pyspark.sql.DataFrameWriter.options')
    optional_args.add_argument("-t", '--table_name', default="temp_table", help="Temporary table name used in the SQL transform script file, defaults to temp_table.")
    optional_args.add_argument("-p", '--script_parameters', default={}, type=lambda x: json.loads(x), 
        help="""Parameter and value pairs to bind to the SQL transform script. eg if the script is "select col1, col2 from temp_table where col1 = '{param1}'", then use {"param1":"id1"} to bind param1""")
    optional_args.add_argument("-v", "--verbose", action="store_true", help="Increase output verbosity.")
    known_args, _ = arg_parser.parse_known_args(args)
    return known_args

# %%
def main (args):
    default_timezone = pytz.timezone('Australia/Melbourne')

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    logger.info(f"Arguments:\n{args}")

    if args.input_path[-1] == '/':
        args.input_path = args.input_path[:-1]
    if args.output_path[-1] == '/':
        args.output_path = args.output_path[:-1]

    if args.run_date:
        path_date = args.run_date.strftime("%Y-%m-%d")
    else:
        path_date = default_timezone.fromutc(datetime.datetime.utcnow() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")

    if args.script_path:
        script_text = load_script(args.script_path)
    else:
        script_text = None

    global spark 
    spark = pyspark.sql.SparkSession.builder\
        .appName("file_converter_pyspark")\
        .config("spark.scheduler.mode", "FAIR")\
        .getOrCreate()
    
    convert_file (spark, 
            source_prefix=f"{args.input_path}/{path_date}/",
            dest_prefix=f"{args.output_path}/{path_date}/",
            source_format=args.input_format,
            dest_format=args.output_format,
            script_text=script_text,
            read_options=args.input_options,
            write_options=args.output_options,
            temp_table_name=args.table_name,
            **args.script_parameters)

# %%
if (__name__ in ['__main__']):
    parsed_args = parse_arguments(sys.argv[1:])
    main(parsed_args)
