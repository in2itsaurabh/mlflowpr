import os
import warnings
import sys

import pandas as pd
import numpy as np
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.model_selection import train_test_split
from urllib.parse import urlparse
import mlflow
import mlflow.sklearn
import dagshub
from sklearn.preprocessing import StandardScaler,OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.linear_model import SGDRegressor

dagshub.init(repo_owner='in2itsaurabh', repo_name='mlflowpr', mlflow=True)
import warnings
warnings.filterwarnings("ignore")
import logging

logging.basicConfig(level=logging.WARN)
logger = logging.getLogger(__name__)


# Setting the environment variable GIT_PYTHON_REFRESH to 'quiet' is related to the behavior of the GitPython library, which is often used in environments where version control with Git is important. 
os.environ['GIT_PYTHON_REFRESH'] = 'quiet'

def eval_metrics(actual, pred):
    rmse = np.sqrt(mean_squared_error(actual, pred))
    mae = mean_absolute_error(actual, pred)
    r2 = r2_score(actual, pred)
    return rmse, mae, r2


if __name__ == "__main__":
    warnings.filterwarnings("ignore")
    np.random.seed(40)

    try:
        df=pd.read_csv('Student_Performance.csv')
    except Exception as e:
        logger.exception(
            "Unable to download training & test CSV, check your internet connection. Error: %s", e
        )


    encoder_feature=['Extracurricular_Activities']
    scaler_feature=['hours', 'scores','Sleep_Hours','Sample_Papers_Practiced']

    preproseccing=ColumnTransformer(
        transformers=[
            ('encoder',OneHotEncoder(),encoder_feature),
        ('scaler',StandardScaler(),scaler_feature)
        ])

    x=df.drop('Performance',axis=1)
    y=df['Performance']

    # Split the data into training and test sets. (0.75, 0.25) split.
    x_train,x_test,y_train,y_test=train_test_split(x,y,test_size=0.2,random_state=42)


    # alpha = float(sys.argv[1]) if len(sys.argv) > 1 else 0.5
    penalty= sys.argv[1] if len(sys.argv) > 1 else "l2" #['l2', 'l1', 'elasticnet'] 

    with mlflow.start_run():
        model=Pipeline([
            ('preproseccing',preproseccing),
            ('lr',SGDRegressor())
            ])
        model.fit(x_train,y_train)

        predicted_qualities = model.predict(x_test)

        (rmse, mae, r2) = eval_metrics(y_test, predicted_qualities)

        print("  SGD Penality:", penalty)
        print("  RMSE: %s" % rmse)
        print("  MAE: %s" % mae)
        print("  R2: %s" % r2)

        mlflow.log_param("penalty", penalty)
        mlflow.log_metric("rmse", rmse)
        mlflow.log_metric("r2", r2)
        mlflow.log_metric("mae", mae)


        # import mlflow
        mlflow.log_param('parameter name', 'value')
        mlflow.log_metric('metric name', 1)
        tracking_url_type_store = urlparse(mlflow.get_tracking_uri()).scheme

        # Model registry does not work with file store
        if tracking_url_type_store != "file":

            # Register the model
            # There are other ways to use the Model Registry, which depends on the use case,
            # please refer to the doc for more information:
            # https://mlflow.org/docs/latest/model-registry.html#api-workflow
            mlflow.sklearn.log_model(model, "model", registered_model_name="SGDRegressor")
        else:
            mlflow.sklearn.log_model(model, "model")