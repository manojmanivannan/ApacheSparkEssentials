
# Data Analysis with Python and PySpark <!-- omit in toc -->

Learning material from the book by Jonathan Rioux

## Table of Contents <!-- omit in toc -->
- [Setup](#setup)
- [Dataset](#dataset)
- [Notebooks](#notebooks)
  - [Part 1 - Get acquainted](#part-1---get-acquainted)
  - [Part 2 - Get proficient](#part-2---get-proficient)
  - [Part 3 - Get confident](#part-3---get-confident)
- [References](#references)
## Setup

You will need Java and Spark installed as per instructions in [here](https://sparkbyexamples.com/spark/apache-spark-installation-on-windows/) for windows and [here](https://sparkbyexamples.com/spark/spark-installation-on-linux-ubuntu/) for linux (ubuntu)

> **Note** Depending on the version of hadoop you get from your spark installation, create a directory, say, c:\hadoop_X\bin. In this directory place `hadoop.dll` and `winutils.exe` after downloading the corresponding verion from this [git repo](https://github.com/kontext-tech/winutils). Then add this directory your Path.

So in summary, you should have set these environment variables
- `HADOOP_HOME=C:\hadoop3`
- `SPARK_HOME=C:\spark-3.2.3-bin-hadoop3`
- `JAVA_HOME=C:\Program Files\Java\jdk1.8.0_202`
- `PYSPARK_PYTHON=python`

Optionally
- `PYSPARK_DRIVER_PYTHON=ipython`
- `PYSPARK_DRIVER_PYTHON_OPTS=notebook`

Finally, you should have appended the following to PATH variable
- `%SPARK_HOME%\bin`
- `%JAVA_HOME%\bin`
- `%HADOOP_HOME%\bin`

For Python dependencies, install necessary packages in a virtual env using `pip install -r requirements.txt`

## Dataset 
Data used in these notes were obtained from [here](https://github.com/jonesberg/DataAnalysisWithPythonAndPySpark) are directed by the author in the book.

## Notebooks

### Part 1 - Get acquainted
 - [1. Introduction](./1_Pyspark_Intro.ipynb)
 - [2. First Program](./2_First_Steps.ipynb)
 - [3. Submitting and Scaling](./3_Scaling.ipynb)
 - [4. Analyse Tabular Data](./4_Analyse_tabular.ipynb)
 - [5. Joining & Grouping](./5_Joining_Grouping.ipynb)
### Part 2 - Get proficient
 - [6. Multidimensional DF: JSON data](./6_PySpark_w_JSON.ipynb)
 - [7. Blending Python and SQL](./7_Python_SQL.ipynb)
 - [8. Extending PySpark w Python: RDD & UDFs](./8_RDD_n_UDFs.ipynb)
 - [9. Big data: Using pandas UDFs](./9_Pandas_UDF.ipynb)
 - [10. Window Functions](./10_Window_Functions.ipynb)
 - [11. Faster PySpark](./11_Faster_PySpark.ipynb)
### Part 3 - Get confident
 - [12. Features for ML](./12_Preparing_ML_Features.ipynb)
 - [13. ML pipelines](./13_Robust_ML_piplines.ipynb)
 - [14. Custom ML transformers](./14_Custom_ML_transformers.ipynb)
## References
- PySpark's [API Documentation](http://spark.apache.org/docs/latest/api/python/)
- Learn [Regular Expression](https://regexr.com/)
- PySpark's [SQL API Documentation](https://spark.apache.org/docs/latest/api/sql/index.html)


