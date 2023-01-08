# Data Analysis with Python and PySpark

Learning material from the book by Jonathan Rioux

## Setup

You will need Java and Spark installed as per instructions in [here](https://sparkbyexamples.com/spark/apache-spark-installation-on-windows/) for windows and [here](https://sparkbyexamples.com/spark/spark-installation-on-linux-ubuntu/) for linux (ubuntu)

> **Note** Depending on the version of hadoop you get from your spark installation, create a directory, say, c:\USER_NAME\hadoop_X_Y\bin. In this directory place `hadoop.dll` and `winutils.exe` after downloading the corresponding verion from this [git repo](https://github.com/kontext-tech/winutils). Then add this directory your Path.

For Python dependencies, install necessary packages using `pip install -r requirements.txt`


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

## References
- PySpark's [API Documentation](http://spark.apache.org/docs/latest/api/python/)
- Learn [Regular Expression](https://regexr.com/)
- PySpark's [SQL API Documentation](https://spark.apache.org/docs/latest/api/sql/index.html)