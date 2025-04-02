# Traffic Accidents Data Processing

## Overview
This project processes traffic accident data from multiple CSV files, normalizes the schema, and stores the cleaned data in Parquet format. The data is sourced from S3 and processed using Apache Spark.

## Features
- Loads raw CSV files from S3 or a local directory
- Normalizes column names and aligns schemas
- Converts data types for accurate analysis
- Joins accident, vehicle, and person data into a unified dataset
- Saves the processed data in Parquet format
- Provides basic analytics, such as total accidents per year

## Project Structure
```
trafficData/
├── data_processing.py  # Functions for data ingestion and transformation
├── main.py             # Main script to orchestrate processing and analytics
├── dataset/            # Directory to store processed data
│   ├── processed/      # Output Parquet files
│   ├── temp_s3_files/  # Temporary storage for S3 downloads
└── README.md           # Project documentation
```

## Requirements
- Python 3.9+
- Apache Spark
- PySpark
- Requests
- AWS S3 (if using remote data source)

## Installation
1. Clone the repository:
   ```sh
   git clone https://github.com/your-repo/trafficData.git
   cd trafficData
   ```
2. Install dependencies:
   ```sh
   pip install -r requirements.txt
   ```
3. Ensure Apache Spark is installed and configured.

## Usage
### Run the data processing pipeline
```sh
python main.py
```
This will:
- Download data from S3 (or read from a local directory)
- Process and clean the data
- Save the output as Parquet
- Display basic analytics

## Future Enhancements
- Implement data quality checks (e.g., missing values, outliers, format validation)
- Add more advanced analytics and visualization
- Optimize performance for large-scale datasets

