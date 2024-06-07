from kafka import KafkaConsumer
import json
from sklearn.linear_model import LinearRegression
import numpy as np
import pandas as pd
from sklearn.metrics import r2_score
from sklearn.preprocessing import StandardScaler,OneHotEncoder
from sklearn.compose import ColumnTransformer

from sklearn.pipeline import Pipeline
from sklearn.linear_model import SGDRegressor
# Initialize Kafka consumer
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
initial_batch_size = 5
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

print("Starting to consume messages...")

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

        df = pd.DataFrame([data_dict])
        x,y=preprocess_data(df)
        model.partial_fit(x,y)
        print("Model trained with current data:")

        y_pred = model.predict(x)[0]
        print(f'Performance is {y_pred:.2f}')
    except json.JSONDecodeError:
        print("Received a message that couldn't be decoded as JSON. Skipping...")
    except Exception as e:
        print(f"An error occurred: {e}")



