from data_processing import init_spark, process_files, load_parquet_files, rename_duplicate_columns

PROCESSED_DATA_PATH = "dataset/processed"


def main():
    # Initialize Spark
    spark = init_spark()

    # Process raw CSV files
    process_files(spark, "acc", source="s3",)
    process_files(spark, "pers", source="s3",)
    process_files(spark, "veh", source="s3",)

    # Load processed Parquet files
    acc_df, veh_df, pers_df = load_parquet_files(spark, PROCESSED_DATA_PATH)

    # Rename duplicate columns
    veh_df = rename_duplicate_columns(veh_df, "veh")
    pers_df = rename_duplicate_columns(pers_df, "pers")

    # Print column names for verification
    print("ACC Columns:", acc_df.columns)
    print("VEH Columns:", veh_df.columns)
    print("PERS Columns:", pers_df.columns)

    # Join tables on "CASENUM"
    joined_df = acc_df.join(veh_df, on="casenum", how="left") \
        .join(pers_df, on="casenum", how="left")

    # Print final schema
    joined_df.printSchema()

    # Save the result as Parquet
    joined_df.write.mode("overwrite").parquet(f"{PROCESSED_DATA_PATH}/traffic_data.parquet")
    print("Final dataset saved.")
    show_analytics(spark)


def show_analytics(spark):
    # Analytics sample
    spark.read.parquet("dataset/processed/traffic_data.parquet").createOrReplaceTempView("traffic")
    result = spark.sql("""
         SELECT YEAR, COUNT(*) AS total_accidents
            FROM traffic
            GROUP BY YEAR
            ORDER BY YEAR DESC
            LIMIT 5
        """)
    result.show()


if __name__ == "__main__":
    main()

