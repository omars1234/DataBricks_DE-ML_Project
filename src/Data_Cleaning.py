from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

import sys
sys.path.append("/Workspace/Users/omars.soub@gmail.com/DataBricks_DE-ML_Project/src")
from pyspark.sql.functions import * # month, when, col
from Data_Ingestion import data_ingestion



def data_cleaning():
    df = data_ingestion()
    df=df.withColumn('month', month(df['Date_sold']))\
        .withColumn("Average_Price_per_Bedroom",when(df["Number_Of_Bedrooms"] > 0, df['price'] / df['Number_Of_Bedrooms'])\
        .otherwise(0))
    df = df.select([col(c).alias(c.capitalize()) for c in df.columns])
    return  df
