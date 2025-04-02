import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col
from pyspark.sql.functions import count, when

import requests


def init_spark(app_name="TrafficAccidents", driver_memory="8g", executor_memory="4g"):
    """
    Initializes a Spark session with given memory settings.
    """
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.driver.memory", driver_memory) \
        .config("spark.executor.memory", executor_memory) \
        .getOrCreate()


def download_file_from_s3(file_url, local_path):
    """Load file from S3 and save local"""
    response = requests.get(file_url)
    if response.status_code == 200:
        with open(local_path, "wb") as f:
            f.write(response.content)
        print(f"Downloaded {file_url} → {local_path}")
    else:
        raise Exception(f"Failed to download {file_url}, status code {response.status_code}")


def process_files(spark, file_prefix, source="local", data_path="dataset/kaggle", output_path="dataset/processed",
                  year_start=16, year_end=21):
    """
    Processes CSV files from a local directory or S3, unifies the schema, and saves as Parquet.


    :param spark: Spark session.
    :param file_prefix: File prefix (e.g., "acc", "pers", "veh").
    :param source: "local" for local files, "s3" for files from S3.
    :param data_path: Directory containing input files (used for local source).
    :param output_path: Directory for saving processed files.
    :param year_start: Year start
    :param year_end: Year end
    """
    S3_BASE_URL = "https://traffic-accident-data.s3.eu-north-1.amazonaws.com/"
    local_s3_path = "dataset/temp_s3_files"  # Local folder to save files from S3
    os.makedirs(local_s3_path, exist_ok=True)

    # Create list of files
    if source == "local":
        file_list = [os.path.join(data_path, f) for f in os.listdir(data_path)
                     if f.startswith(file_prefix) and f.endswith(".csv")]
    elif source == "s3":
        file_list = []
        for year in range(year_start, year_end):
            s3_url = f"{S3_BASE_URL}{file_prefix}_{year}.csv"
            local_file = os.path.join(local_s3_path, f"{file_prefix}_{year}.csv")
            download_file_from_s3(s3_url, local_file)
            file_list.append(local_file)
    else:
        raise ValueError("Invalid source. Use 'local' or 's3'.")

    if not file_list:
        print(f"No files found for prefix '{file_prefix}' from {source}")
        return None

    print(f"Processing {len(file_list)} files for prefix '{file_prefix}' from {source}")

    # Identify unique columns (convert to lowercase for consistency)
    df_sample = spark.read.csv(file_list[0], header=True, inferSchema=True)
    final_columns = sorted([col.lower() for col in df_sample.columns])

    # # Function to load a file and align its schema
    def load_and_align_schema(file_path):
        df = spark.read.csv(file_path, header=True, inferSchema=True)
        df = df.toDF(*[col.lower() for col in df.columns])  # lowercase
        for col_name in final_columns:
            if col_name not in df.columns:
                df = df.withColumn(col_name, lit(None))  # Add empty value to absent columns
        return df.select(final_columns)  # ordered columns

    # Concatenate all files
    df_all = spark.read.csv(file_list[0], header=True, inferSchema=True).toDF(*final_columns)
    for file in file_list[1:]:
        df = load_and_align_schema(file)
        df_all = df_all.unionByName(df, allowMissingColumns=True)

    df_all.printSchema()

    # Save to Parquet
    output_file = f"{output_path}/{file_prefix}.parquet"
    df_all.write.mode("overwrite").parquet(output_file)
    print(f"Saved unified dataset: {output_file}")

    return df_all


def load_parquet_files(spark, processed_data_path):
    """
    Loads processed Parquet files and performs data type conversions.

    :param spark: Spark session.
    :param processed_data_path: Path to processed Parquet files.
    :return: DataFrames for accidents, vehicles, and persons.
    """
    acc_df = spark.read.parquet(f"{processed_data_path}/acc.parquet")
    veh_df = spark.read.parquet(f"{processed_data_path}/veh.parquet")
    pers_df = spark.read.parquet(f"{processed_data_path}/pers.parquet")

    # Convert data types
    acc_df = acc_df.withColumn("year", col("year").cast("int")) \
        .withColumn("month", col("month").cast("int")) \
        .withColumn("hour", col("hour").cast("int"))

    veh_df = veh_df.withColumn("mod_year", col("mod_year").cast("int")) \
        .withColumn("numoccs", col("numoccs").cast("int"))

    pers_df = pers_df.withColumn("age", col("age").cast("int")) \
        .withColumn("inj_sev", col("inj_sev").cast("int"))

    return acc_df, veh_df, pers_df


def rename_duplicate_columns(df, prefix):
    """
    Renames duplicate columns by adding a prefix, except for "casenum".

    :param df: Spark DataFrame.
    :param prefix: Prefix for column names.
    :return: DataFrame with renamed columns.
    """
    return df.select([col(c).alias(f"{prefix}_{c}") if c != "casenum" else col(c) for c in df.columns])


def check_data_quality(df, table_name, year_start, year_end):
    """
    Performs data quality checks on a DataFrame.

    :param df: Spark DataFrame.
    :param table_name: Name of the table (for logging purposes).
    """
    min_age = 0
    max_age = 120
    print(f"\n=== Data Quality Check: {table_name} ===\n")

    # Check missed values
    print("Missing values per column:")
    df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).show()

    # Check anomalies
    if "year" in df.columns:
        print(f"Invalid YEAR values (should be between {year_start} and {year_end}):")
        df.filter((col("year") < year_start) | (col("year") > year_end)).select("casenum", "year").show()

    if "age" in df.columns:
        print(f"Invalid AGE values (should be between {min_age} and {max_age}):")
        df.filter((col("age") < min_age) | (col("age") > max_age)).select("casenum", "age").show()

    if "casenum" in df.columns:
        print("Checking CASENUM format (should be numeric):")
        df.filter(~col("casenum").rlike("^[0-9]+$")).select("casenum").show()

    # Check duplicates
    print("Checking for duplicate CASENUMs:")
    df.groupBy("casenum").count().filter(col("count") > 1).show()