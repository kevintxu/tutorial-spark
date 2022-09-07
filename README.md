# tutorial-spark

## Apache Spark in Python: Beginner's Guide
https://www.datacamp.com/tutorial/apache-spark-python

## Getting pyspark to work with Windows 10

1. Install Miniconda
2. Install pyspark using Miniconda. Currently only version 2.3.2 works, so the install code should be `conda install -c conda-forge pyspark=2.3.2`
3. Download winutils from https://github.com/steveloughran/winutils. Extract the hadoop folder. Set two user environment variables: `HADOOP_HOME=<path to hadoop folder>` and `PATH=%PATH%;<path to hadoop folder>\bin`.
4. Make sure Java (use version 8 to ensure compatibility) is installed and is in the PATH environment variable.
5. Submit pyspark script to spark (either to local instance or remote instance). For example, to submit the `hello_world.py` to run locally: `spark-submit --deploy-mode client <some_path>\hello_world.py`
