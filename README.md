# Apache Spark Essentials <!-- omit in toc -->

Learning in use Spark in the context of Python (PySpark) and how to use it in the realm of data analytics.

## Table of Contents <!-- omit in toc -->
- [Setup \& Configuration](#setup--configuration)
- [Spark Example code](#some-spark-example-scala-vs-pyspark)
- **[Data Analytics using PySpark](DataAnalysis/README.md)**
- **Offline notebooks**
  - [Basics](./BasicOperations.ipynb)
- **Online course notebooks** - Colab (Linux) only
  - Linkedin
    - [Basics](./SparkByLinkedin.ipynb) 
  - PluralSight
    - [Basics](./SparkByPluralsight.ipynb)
    - [Stream data](./SparkStreamByPluralsight.ipynb)
    - [ML using old library](./ML_SparkByPluralsight.ipynb)
    - [ML using new library](./ML_2_SparkByPluralsight.ipynb)

## Setup & Configuration

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


---

## Some Spark example (Scala Vs PySpark)

Open a new command-prompt and start spark shell using command `spark-shell` or `pyspark` to start pyspark session on windows

```python

#### Read a csv into dataframe using
scala> val df = spark.read.format("csv").option("header","true").option("inferSchema","true").load("data/online_retail.csv")
pyspark >>> df = spark.read.load("data/online_retail.csv",format="csv",header="true",inferSchema="true")

### Print the schema
scala> df.printSchema()`
pyspark>>> df.printSchema()`
### Select a column
scala> df.select("Country").distinct().show()`
pyspark>>> df.select("Country").distinct().show()`

### Aggregation of dataframe
scala> df.select(df("InvoiceNo"),df("UnitPrice")*df("Quantity")).groupBy("InvoiceNo").sum().show()`
pyspark>>> df.select(df["InvoiceNo"],df["UnitPrice"]*df["Quantity"]).groupBy("InvoiceNo").sum().show()`

### Filter of dataframe
scala>  df.filter(df("InvoiceNo")===536415).show()`
pyspark>>> df.filter(df["InvoiceNo"]==536415).show()`


### Top 10 products in the UK
scala> (df.select(
          df("Country"), 
          df("Description"),
          (df("UnitPrice")*df("Quantity"))
          .alias("Total"))
          .groupBy("Country", "Description")
          .sum()
          .filter(df("Country")=="United Kingdom")
          .sort(desc("sum(Total)"))
          .limit(10)
          .show()
          )
pyspark>>> (df.select(
              df["Country"], 
              df["Description"],
              (df["UnitPrice"]*df["Quantity"])
              .alias("Total"))
              .groupBy("Country", "Description")
              .sum()
              .filter(df["Country"]=="United Kingdom")
              .sort("sum(Total)", ascending=False)
              .limit(10)
            )

# Saving dataframe to table

scala>  val r1 = df.select(df("Country"), df("Description"),(df("UnitPrice")*df("Quantity")).alias("Total"))
scala> r1.write.saveAsTable("data/online_retail_prod_sales_country") # will trow errror

pyspark>>> r1 = df.select(df["Country"], df["Description"],(df["UnitPrice"]*df["Quantity"]).alias("Total"))
pyspark>>> r1.write.saveAsTable("data/online_retail_prod_sales_country") # will trow errror
```