# %%
import pyspark
import sys

# %%
from .file_converter_pyspark import main, parse_arguments

if (__name__ in ['__main__']):
    parsed_args = parse_arguments(sys.argv[1:])
    main(parsed_args)
