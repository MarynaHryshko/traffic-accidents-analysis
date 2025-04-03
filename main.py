import logging
from data_processing import (
    init_spark, process_files, load_parquet_files,
    rename_duplicate_columns, check_data_quality
)

# Config
PROCESSED_DATA_PATH = "dataset/processed"
TRAFFIC_DATA_FILE = f"{PROCESSED_DATA_PATH}/traffic_data.parquet"
YEAR_FROM = 2016
YEAR_TO = 2021

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def main():
    logging.info("\n=== Initializing Spark session ===\n")
    spark = init_spark()

    year_from = 2016
    year_to = 2021

    # Process raw files
    for prefix in ["acc", "pers", "veh"]:
        try:
            process_files(spark, prefix, source="s3", year_start=year_from, year_end=year_to)
        except Exception as e:
            logging.error(f"Failed to process {prefix}: {e}")

    # Load processed files
    acc_df, veh_df, pers_df = load_parquet_files(spark, PROCESSED_DATA_PATH)

    # Check data quality
    for df, name in zip([acc_df, veh_df, pers_df], ["Accidents", "Vehicles", "Persons"]):
        check_data_quality(df, name, year_from, year_to)

    # Rename columns with duplicate names
    veh_df = rename_duplicate_columns(veh_df, "veh")
    pers_df = rename_duplicate_columns(pers_df, "pers")

    logging.info(f"ACC Columns: {acc_df.columns}")
    logging.info(f"VEH Columns: {veh_df.columns}")
    logging.info(f"PERS Columns: {pers_df.columns}")

    # Join tables
    joined_df = acc_df.join(veh_df, on="casenum", how="left") \
        .join(pers_df, on="casenum", how="left")

    logging.info("Final schema:")
    joined_df.printSchema()

    # Save to Parquet
    try:
        joined_df.write.mode("overwrite").parquet(TRAFFIC_DATA_FILE)
        logging.info(f"Final dataset saved: {TRAFFIC_DATA_FILE}")
    except Exception as e:
        logging.error(f"Failed to save final dataset: {e}")

    # Show analytics
    show_analytics(spark, TRAFFIC_DATA_FILE)


def show_analytics(spark, data_file):
    try:
        spark.read.parquet(data_file).createOrReplaceTempView("traffic")
        result = spark.sql("""
            SELECT YEAR, COUNT(*) AS total_accidents
            FROM traffic
            GROUP BY YEAR
            ORDER BY YEAR DESC
            LIMIT 5
        """)
        result.show()
    except Exception as e:
        logging.error(f"Failed to perform analytics: {e}")


if __name__ == "__main__":
    main()
