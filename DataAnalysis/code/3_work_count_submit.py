from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, lower, regexp_extract, split
import pathlib
import os

spark = SparkSession.builder.appName(
    "Analyzing the vocabulary of Pride and Prejudice."
).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# data_path = os.path.join(pathlib.Path(__file__).parent, "../data/gutenberg_books/1342-0.txt")
data_path = os.path.join(pathlib.Path(__file__).parent, "../data/gutenberg_books/*txt")
out_path = os.path.join(pathlib.Path(__file__).parent, "../data/gutenberg_books/simple_count_single_partition.csv")

book = spark.read.text(data_path)

lines = book.select(split(book.value, " ").alias("line"))

words = lines.select(explode(col("line")).alias("word"))

words_lower = words.select(lower(col("word")).alias("word"))

words_clean = words_lower.select(
    regexp_extract(col("word"), "[a-z']*", 0).alias("word")
)

words_nonull = words_clean.where(col("word") != "")

results = words_nonull.groupby(col("word")).count()

results.orderBy("count", ascending=False).show(10)

results.coalesce(1).write.mode("overwrite").csv(out_path)
