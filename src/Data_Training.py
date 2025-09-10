from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

import sys
sys.path.append("/Workspace/Users/omars.soub@gmail.com/DataBricks_DE-ML_Project/src")
from Data_Transformation import data_transformation

import mlflow
!pip install scikit-learn
!pip install numpy

import sklearn
import numpy as np
from sklearn.model_selection import KFold
from sklearn.ensemble import (
    RandomForestRegressor, GradientBoostingRegressor, 
    AdaBoostRegressor, BaggingRegressor
)
from sklearn.neighbors import KNeighborsRegressor
from sklearn.tree import DecisionTreeRegressor
from sklearn.metrics import mean_squared_error

# Set the registry URI to databricks
mlflow.set_registry_uri("databricks")

preprocessed_data, y = data_transformation()

with mlflow.start_run(run_name="Cross-Validation Experiment-House Pricing Project2"):
    kf = KFold(n_splits=2, shuffle=True, random_state=42)

    models = {
        "RandomForestRegressor": RandomForestRegressor(),
        "GradientBoostingRegressor": GradientBoostingRegressor(),
        "AdaBoostRegressor": AdaBoostRegressor(),
        "KNeighborsRegressor": KNeighborsRegressor(),
        "BaggingRegressor": BaggingRegressor(DecisionTreeRegressor(), n_estimators=500)
    }

    for model_name, model_instance in models.items():
       
        with mlflow.start_run(nested=True, run_name=model_name):
            mlflow.log_param("model_type", model_name)
            fold_metrics = []

            for fold_idx, (train_index, test_index) in enumerate(kf.split(preprocessed_data)):
                with mlflow.start_run(nested=True, run_name=f"{model_name}_Fold_{fold_idx}"):
                    X_train, X_test = preprocessed_data[train_index], preprocessed_data[test_index]
                    y_train, y_val = y[train_index], y[test_index]

                    model_instance.fit(X_train, y_train)
                    predictions = model_instance.predict(X_test)

                    rmse = np.sqrt(mean_squared_error(y_val, predictions))
                    mlflow.log_metric("RMSE", rmse)
                    fold_metrics.append(rmse)

            avg_rmse = np.mean(fold_metrics)
            mlflow.log_metric("avg_RMSE", avg_rmse)
