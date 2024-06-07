from kafka import KafkaProducer
import json

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

data = input_data(7, 50, 'yes', 14, 0, 40)

try:
    producer.send('student-performance', data)
    producer.flush()  # Ensure all messages are sent before 
    print(f"Sent: {data}")
except Exception as e:
    print(f"Failed to send data: {e}")

# Close the producer
# producer.close()
