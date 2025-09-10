from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

path = "workspace.homepricesprojectdatawarehouse.goldenhouseprices_info_view"

def data_ingestion():
    df = spark.read.table(path)
    return df