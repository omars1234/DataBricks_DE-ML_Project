
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

import sys
sys.path.append("/Workspace/Users/omars.soub@gmail.com/DataBricks_DE-ML_Project/src")
from Data_Cleaning import data_cleaning

from sklearn.preprocessing import StandardScaler,MinMaxScaler,PowerTransformer,OneHotEncoder,OrdinalEncoder
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer


def data_transformation():
    df = data_cleaning()
    dfpd = df.toPandas() ## convert to pandas

    x=dfpd.drop(["Date_sold","Price","Month_name"],axis=1)
    y=dfpd["Price"]

    numeric_transformer = MinMaxScaler()
    oneh_categorical_transformer=OneHotEncoder(sparse_output=False)
    Ordinal_categorical_transformer=OrdinalEncoder()

    transform_columns = ["Average_price_per_bedroom"]
    num_features = ["Average_price_per_bedroom"]
    oneh_features=["Region_postcode","Property_type"]
    Ordinal_features=["Number_of_bedrooms","Month","Day_name","Year_date"]

    transform_pipe = Pipeline(steps=[
                    ('transformer', PowerTransformer(method='yeo-johnson'))

                ])
    preprocessor = ColumnTransformer(
                    [
                        ("Transformer", transform_pipe, transform_columns),
                        ("StandardScaler", numeric_transformer, num_features),
                        ("OrdinalEncoder", Ordinal_categorical_transformer, Ordinal_features),
                        ("OneHotEncoder", oneh_categorical_transformer, oneh_features),
                        
                    ]
                )

    preprocessed_data=preprocessor.fit_transform(x)
    return preprocessed_data,y    
      