# Apache Spark Essentials


## Setup & Configuration

### For Windows

After downloading Apache spark binary,

1) Puts Spark in a path variable so you can execute commandlets from that directory.  Adjust to your own directory.
`PATH=PATH;C:\Users\YOUR_NAME\spark<VERSION-NO>\bin\"`

2) Set vars for PySpark
`PYSPARK_DRIVER_PYTHON="ipython"`

3) Specifies PySpark options
`PYSPARK_DRIVER_PYTHON_OPTS="notebook"`

4) Download `winutils.exe` and `hadoop.dll` from https://github.com/steveloughran/winutils and save the folder of hadoop somewhere in your PC and load that path in env variables as HADOOP_HOME
`$env:HADOOP_HOME = "C:\Users\YOUR_NAME\hadoop"`

You can apply step 1 through 4 using the GUI on editing system environment variables

---

## Spark
Open a new command-prompt and start spark shell using command `spark-shell` or `pyspark` to start pyspark session on windows

#### Read a csv into dataframe using
`scala> val df = spark.read.format("csv").option("header","true").option("inferSchema","true").load("data/online_retail.csv")`

`pyspark >>> df = spark.read.load("data/online_retail.csv",format="csv",header="true",inferSchema="true")`

#### Print the schema
`scala> df.printSchema()`
`pyspark>>> df.printSchema()`

#### Select a column
`scala> df.select("Country").distinct().show()`
`pyspark>>> df.select("Country").distinct().show()`

#### Aggregation of dataframe
`scala> df.select(df("InvoiceNo"),df("UnitPrice")*df("Quantity")).groupBy("InvoiceNo").sum().show()`
`pyspark>>> df.select(df["InvoiceNo"],df["UnitPrice"]*df["Quantity"]).groupBy("InvoiceNo").sum().show()`

#### Filter of dataframe
`scala>  df.filter(df("InvoiceNo")===536415).show()`
`pyspark>>> df.filter(df["InvoiceNo"]==536415).show()`


#### Top 10 products in the UK
`scala> df.select(df("Country"), df("Description"),(df("UnitPrice")*df("Quantity")).alias("Total")).groupBy("Country", "Description").sum().filter(df("Country")==="United Kingdom").sort(desc("sum(Total)")).limit(10).show()`
`pyspark>>> df.select(df["Country"], df["Description"],(df["UnitPrice"]*df["Quantity"]).alias("Total")).groupBy("Country", "Description").sum().filter(df["Country"]=="United Kingdom").sort("sum(Total)", ascending=False).limit(10)`

#### Saving dataframe to table

`scala>  val r1 = df.select(df("Country"), df("Description"),(df("UnitPrice")*df("Quantity")).alias("Total"))`
`scala> r1.write.saveAsTable("data/online_retail_prod_sales_country")` # will trow errror
`pyspark>>> r1 = df.select(df["Country"], df["Description"],(df["UnitPrice"]*df["Quantity"]).alias("Total"))`
`pyspark>>> r1.write.saveAsTable("data/online_retail_prod_sales_country")` # will trow errror