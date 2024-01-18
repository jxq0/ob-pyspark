import os
import pandas as pd


def init_spark():
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    session = (
        SparkSession.builder.master("local[5]")
        .config("spark.driver.bindAddress", "localhost")
        .config("spark.network.timeout", 10000000)
        .getOrCreate()
    )

    return session


def read_files(spark):
    for pair_str in input_files.split(","):
        pair = pair_str.split(":")
        file_path = ""
        table_name = ""
        if len(pair) == 2:
            file_path = pair[0]
            table_name = pair[1]
        else:
            file_path = pair[0]
            table_name = os.path.splitext(os.path.basename(file_path))[0]

        file_extension = os.path.splitext(file_path)[1]

        reader = spark.read
        if file_extension == ".csv":
            reader.csv(
                file_path,
                header=True,
                inferSchema=True,
            ).createOrReplaceTempView(table_name)
        elif file_extension == ".json":
            reader.json(file_path, multiLine=True).createOrReplaceTempView(
                table_name
            )
        elif file_extension in [".xlsx", "xls"]:
            pdf = pd.read_excel(file_path)
            spark.createDataFrame(pdf).createOrReplaceTempView(table_name)
        else:
            raise ValueError("Unknown file type")


def df_to_table(df):
    table = list()
    table.append(df.columns)
    table.append(None)

    for row in df.collect():
        table.append([row[col] for col in df.columns])

    return table


def run():
    spark = init_spark()
    if input_files:
        read_files(spark)

    df = spark.sql(sql)
    if output_table:
        df.createOrReplaceTempView(output_table)

    return df_to_table(df)


run()
