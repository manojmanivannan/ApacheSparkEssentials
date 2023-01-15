# Data Analysis with Python and PySpark

Learning material from the book by Jonathan Rioux

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


Data obtained from [here](https://github.com/jonesberg/DataAnalysisWithPythonAndPySpark)
---
## Notebooks

1. Part A
    1. [Introduction](./1_Pyspark_Intro.ipynb)
    2. [First Program](./2_First_Steps.ipynb)
    3. [Submitting and Scaling](./3_Scaling.ipynb)
    4. [Analyse Tabular Data](./4_Analyse_tabular.ipynb)
    5. [Joining & Grouping](./5_Joining_Grouping.ipynb)
2. Part B
    1. [Multidimensional DF: JSON data](./6_PySpark_w_JSON.ipynb)
    2. [Blending Python and SQL](./7_Python_SQL.ipynb)
    3. [Extending PySpark w Python: RDD & UDFs](./8_RDD_n_UDFs.ipynb)

## References
- PySpark's [API Documentation](http://spark.apache.org/docs/latest/api/python/)
- Learn [Regular Expression](https://regexr.com/)
- PySpark's [SQL API Documentation](https://spark.apache.org/docs/latest/api/sql/index.html)