import os
import warnings
import sys

import pandas as pd
import numpy as np
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from urllib.parse import urlparse
import mlflow
import mlflow.sklearn
import dagshub
from sklearn.pipeline import Pipeline
from kafka import KafkaConsumer
import json
import numpy as np
import pandas as pd
from sklearn.metrics import r2_score
from sklearn.preprocessing import StandardScaler,OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.linear_model import SGDRegressor

dagshub.init(repo_owner='in2itsaurabh', repo_name='mlflowpr', mlflow=True)
import warnings
warnings.filterwarnings("ignore")
import logging

logging.basicConfig(level=logging.WARN)
logger = logging.getLogger(__name__)

os.environ['GIT_PYTHON_REFRESH'] = 'quiet'


consumer = KafkaConsumer('student-performance',
                         bootstrap_servers='localhost:9092',
                         auto_offset_reset='earliest',
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))


# Initialize model
model = SGDRegressor()


encoder_feature=['Extracurricular_Activities']
scaler_feature=['hours', 'scores','Sleep_Hours','Sample_Papers_Practiced']

preproseccing=ColumnTransformer(
    transformers=[
    ('encoder',OneHotEncoder(drop='first'),encoder_feature),
    ('scaler',StandardScaler(),scaler_feature)
    ])

def preprocess_data(df, fit=False):
    x = df.drop('Performance', axis=1)
    y = df['Performance']
    if fit:
        x_trans = preproseccing.fit_transform(x)
    else:
        x_trans = preproseccing.transform(x)
    x_trans_df = pd.DataFrame(x_trans,columns=x.columns)
    return x_trans_df, y

data_list=[]
initial_batch_size = 100
for _ in range(initial_batch_size):
    message = next(consumer)
    data = message.value
    print(f"Received data: {data}")
    
    data_dict = {
        'hours': data['hours'],
        'scores': data['scores'],
        'Extracurricular_Activities': data['Extracurricular_Activities'],
        'Sleep_Hours': data['Sleep_Hours'],
        'Sample_Papers_Practiced': data['Sample_Papers_Practiced'],
        'Performance': data['Performance']
    }
    # print(data_dict.keys())
    data_list.append(data_dict)

# Initial fitting of the preprocessing pipeline
df_initial = pd.DataFrame(data_list)

X_initial, y_initial = preprocess_data(df_initial, fit=True)
model.fit(X_initial, y_initial)

def eval_metrics(actual, pred):
    rmse = np.sqrt(mean_squared_error(actual, pred))
    mae = mean_absolute_error(actual, pred)
    r2 = r2_score(actual, pred)
    return rmse, mae, r2


print("Starting to consume messages...")





if __name__ == "__main__":
    warnings.filterwarnings("ignore")
    np.random.seed(40)

    for message in consumer:
        try:
            data = message.value
            print(f"Received data: {data}")

            data_dict = {
                'hours':data['hours'],
                'scores':data['scores'],
                'Extracurricular_Activities':data['Extracurricular_Activities'],
                'Sleep_Hours':data['Sleep_Hours'],
                'Sample_Papers_Practiced':data['Sample_Papers_Practiced'],
                'Performance':data['Performance']
            }
            with mlflow.start_run():
                df = pd.DataFrame([data_dict])
                x,y=preprocess_data(df)
                model.partial_fit(x,y)
                print("Model trained with current data:")

                predicted_qualities = model.predict(x)

                (rmse, mae, r2) = eval_metrics(y, predicted_qualities)

                print("  RMSE: %s" % rmse)
                print("  MAE: %s" % mae)
                print("  R2: %s" % r2)

                mlflow.log_metric("rmse", rmse)
                mlflow.log_metric("r2", r2)
                mlflow.log_metric("mae", mae)

                # import mlflow
                mlflow.log_param('parameter name', 'value')
                mlflow.log_metric('metric name', 1)
                tracking_url_type_store = urlparse(mlflow.get_tracking_uri()).scheme
                if tracking_url_type_store != "file":
                    mlflow.sklearn.log_model(model, "model",registered_model_name="SGDRegressor")
                else:
                    mlflow.sklearn.log_model(model, "model")
                
        except json.JSONDecodeError:
            print("Received a message that couldn't be decoded as JSON. Skipping...")
        except Exception as e:
            print(f"An error occurred: {e}")

