from kafka import KafkaProducer
import json
import pandas as pd
import numpy as np

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def input_data(hours, scores, Extracurricular_Activities, Sleep_Hours, Sample_Papers_Practiced,Performance):
    data = {
        'hours': hours,
        'scores': scores,
        'Extracurricular_Activities': Extracurricular_Activities,
        'Sleep_Hours': Sleep_Hours,
        'Sample_Papers_Practiced': Sample_Papers_Practiced,
        'Performance':Performance
    }
    return data
df=pd.read_csv('Student_Performance.csv')
try:
    for i in range(len(df)):
        l1=df.iloc[i:i+1].values[0].tolist()
        data=input_data(*l1)
        producer.send('student-performance', data)
        producer.flush()  # Ensure all messages are sent before 
        print(f"Sent: {data}")
except Exception as e:
    print(f"Failed to send data: {e}")

# Close the producer
# producer.close()
