from src.util.constants import DATASET_PATH, KPI_ROOT_PATH
from src.util.spark_util import get_spark_session
from pyspark.sql.functions import *
import argparse

kpi = {
    'top_5_crime_districts': 'select district_name, count(*) as crimes_number from dataset group by district_name order by crimes_number desc limit 5',
    'top_5_safe_districts': 'select district_name, count(*) as crimes_number from dataset group by district_name order by crimes_number limit 5',
    'crimes_by_crime_type': 'select crimeType, count(*) as crimes_number from dataset group by crimeType',
    'different_crimes': 'select distinct(crimeId), Longitude, Latitude, crimeType, lastOutcome  from dataset',
    'different_crime_types': 'select distinct crimeType as crime_types from dataset',
    'top_5_crime_types_by_district': 'select t2.district_name, t2.crimeType from (select t1.crimeType, t1.district_name, row_number() over(partition by t1.district_name order by temp desc) as rn from ' +
                                     '(select crimeType, district_name, count(*) over(partition by district_name, crimeType) as temp from dataset) t1) t2 where t2.rn <= 5'
}


def main(metrics):
    spark = get_spark_session()
    input_dataset = spark.read.parquet(DATASET_PATH)
    input_dataset.createOrReplaceTempView("dataset")

    for metric in metrics:
        if metric in kpi:
            aggregated_dataset = input_dataset.sql_ctx.sql(kpi[metric])
            aggregated_dataset.select(to_json(struct(*aggregated_dataset.columns)).alias("json")) \
                .withColumn("kpi_metric", lit(metric)) \
                .coalesce(1) \
                .groupBy(col("kpi_metric")) \
                .agg(collect_list("json").alias("results")) \
                .select("kpi_metric", "results") \
                .write.mode("overwrite").json(
                f"{KPI_ROOT_PATH}/{metric}")
        else:
            print("Wrong KPI Name")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Args for script')
    parser.add_argument('--metrics', type=lambda s: [i for i in s.split(',')])

    args = parser.parse_args()
    main(args.metrics)
