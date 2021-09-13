import os
import argparse

from src.util.spark_util import get_spark_session
from pyspark.sql.functions import *

spark = get_spark_session()


def read_and_normalize_streets(path, district_name):
    streets_df = spark.read.option("header", True).csv(path)

    return streets_df.select("Crime ID", "Longitude", "Latitude", "Crime type", "Last outcome category") \
        .withColumnRenamed("Crime ID", "crimeId") \
        .withColumnRenamed("Crime type", "crimeType") \
        .withColumnRenamed("Last outcome category", "streets_lastOutcome") \
        .withColumn("district_name", lit(district_name))


def read_and_normalize_outcomes(path):
    outcomes_df = spark.read.option("header", True).csv(path)

    return outcomes_df.select("Crime ID", "Outcome type") \
        .withColumnRenamed("Crime ID", "crimeId") \
        .withColumnRenamed("Outcome type", "outcomeType")


def write_dataset_as_parquet(df, path):
    df.coalesce(1) \
        .write \
        .partitionBy("district_name", "crimeType", "lastOutcome") \
        .mode("append") \
        .parquet(path, compression='gzip')


def extract_district(line):
    return line.replace("-street.csv", "")[8:]


def main(input_csv_root_folder, output_parquet_folder):
    for item in os.listdir(input_csv_root_folder):
        if ".DS_Store" not in item:
            nested = os.listdir(f"{input_csv_root_folder}/{item}")
            outcomes = set()
            streets = set()

            for element in nested:
                if 'outcomes' in element:
                    outcomes.add(element)
                elif 'street' in element:
                    streets.add(element)
                else:
                    continue

            for street_path in streets:
                district_name = extract_district(street_path)

                current_street_df = read_and_normalize_streets(f"{input_csv_root_folder}/{item}/{street_path}",
                                                               district_name)
                result_df = current_street_df

                outcome_path = street_path.replace("-street", "-outcomes")

                if outcome_path in outcomes:
                    current_outcome_df = read_and_normalize_outcomes(f"{input_csv_root_folder}/{item}/{outcome_path}")
                    result_df = result_df.join(current_outcome_df, ['crimeId'], "left")

                    result_df = result_df.select("crimeId", "district_name", "Longitude", "Latitude", "crimeType",
                                                 when(col("outcomeType").isNotNull(), col("outcomeType")).otherwise(
                                                     col("streets_lastOutcome")).alias("lastOutcome"))

                    write_dataset_as_parquet(result_df, output_parquet_folder)

                else:
                    result_df = result_df.withColumnRenamed("streets_lastOutcome", "lastOutcome")
                    write_dataset_as_parquet(result_df, output_parquet_folder)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Args for script')
    parser.add_argument('--input_csv_path', type=str)
    parser.add_argument('--output_parquet_path', type=str)

    args = parser.parse_args()
    main(args.input_csv_path, args.output_parquet_path)
