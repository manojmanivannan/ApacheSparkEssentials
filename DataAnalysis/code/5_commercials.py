#  5_commercials.py #############################################################
#
# This program computes the commercial ratio for each channel present in the
# dataset.
#
###############################################################################

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pathlib
import os

spark = SparkSession.builder.appName("CRTC").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

data_parent = pathlib.Path(__file__).parent

###############################################################################
# Reading all the relevant data sources
###############################################################################


logs = spark.read.csv(
    os.path.join(data_parent,"../data/broadcast_logs/BroadcastLogs_2018_Q3_M8_sample.csv"),
    sep="|",
    header=True,
    inferSchema=True,
    encoding='utf-8'
)


log_identifier = spark.read.csv(
    os.path.join(data_parent,"../data/broadcast_logs/ReferenceTables/LogIdentifier.csv"),
    sep="|",
    header=True,
    inferSchema=True,
    encoding='utf-8'
)

cd_category = spark.read.csv(
    os.path.join(data_parent,"../data/broadcast_logs/ReferenceTables/CD_Category.csv"),
    sep="|",
    header=True,
    inferSchema=True,
    encoding='utf-8'
).select(
    "CategoryID",
    "CategoryCD",
    F.col("EnglishDescription").alias("Category_Description"),
)

cd_program_class = spark.read.csv(
    os.path.join(data_parent,"../data/broadcast_logs/ReferenceTables/CD_ProgramClass.csv"),
    sep="|",
    header=True,
    inferSchema=True,
    encoding='utf-8'
).select(
    "ProgramClassID",
    "ProgramClassCD",
    F.col("EnglishDescription").alias("ProgramClass_Description"),
)

###############################################################################
# Data processing
###############################################################################

logs = logs.drop("BroadcastLogID", "SequenceNO")

logs = logs.withColumn(
    "duration_seconds",
    (
        F.col("Duration").substr(1, 2).cast("int") * 60 * 60
        + F.col("Duration").substr(4, 2).cast("int") * 60
        + F.col("Duration").substr(7, 2).cast("int")
    ),
)

log_identifier = log_identifier.where(F.col("PrimaryFG") == 1)

logs_and_channels = logs.join(log_identifier, "LogServiceID")

full_log = logs_and_channels.join(cd_category, "CategoryID", how="left").join(
    cd_program_class, "ProgramClassID", how="left"
)

answer = full_log.groupby("LogIdentifierID").agg(
    F.sum(
        F.when(
            F.trim(F.col("ProgramClassCD")).isin(
                ["COM", "PRC", "PGI", "PRO", "LOC", "SPO", "MER", "SOL"]
            ),
            F.col("duration_seconds"),
        ).otherwise(0)
    ).alias("duration_commercial"),
    F.sum("duration_seconds").alias("duration_total"),
).withColumn(
    "commercial_ratio", F.col("duration_commercial") / F.col("duration_total")
)


answer.orderBy(
    "commercial_ratio", ascending=False
).show(
    1000, False
)

answer.coalesce(1).write.mode("overwrite").option("header", "true").csv(
    os.path.join(pathlib.Path(__file__).parent, 
    "../data/broadcast_logs/answer.csv")
)