from src.util.constants import DATASET_PATH, OUTPUT_DATASET_PATH
from src.util.spark_util import get_spark_session
from pyspark.sql.functions import *
import argparse


def main(districts=None, crime_types=None, outcomes=None):
    spark = get_spark_session()
    input_dataset = spark.read.parquet(DATASET_PATH)

    if districts is not None:
        input_dataset = input_dataset.where(col("district_name").isin(districts))
    if crime_types is not None:
        input_dataset = input_dataset.where(col("crimeType").isin(crime_types))
    if outcomes is not None:
        input_dataset = input_dataset.where(col("lastOutcome").isin(outcomes))

    input_dataset.write.mode("overwrite").json(OUTPUT_DATASET_PATH)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Args for script')
    parser.add_argument('--districts', type=lambda s: [i for i in s.split(',')])
    parser.add_argument('--crime_types', type=lambda s: [i for i in s.split(',')])
    parser.add_argument('--outcomes', type=lambda s: [i for i in s.split(',')])

    args = parser.parse_args()
    main(args.districts, args.crime_types, args.outcomes)
