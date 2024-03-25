# Databricks notebook source
import random
import datetime

# Generate production lines dataset
production_lines = []
num_production_lines = 100
for i in range(num_production_lines):
    production_line = {
        'id': i + 1,
        'creation_date': datetime.datetime.now() - datetime.timedelta(days=random.randint(0, 365)),
        'city_name': random.choice(['New York', 'London', 'Paris', 'Tokyo', 'Sydney']),
        'size': random.choice(['Small', 'Medium', 'Large']),
        'other_info': 'Lorem ipsum dolor sit amet',
    }
    production_lines.append(production_line)

# Generate sensor data
sensor_data = []
num_sensors = 50
start_date = datetime.datetime.now() - datetime.timedelta(days=30)
end_date = datetime.datetime.now()
current_date = start_date

while current_date < end_date:
    for i in range(num_sensors):
        sensor = {
            'sensor_id': i + 1,
            'timestamp': current_date,
            'humidity': random.uniform(0, 100),
            'pressure': random.uniform(900, 1100),
            'temperature': random.uniform(20, 40),
        }
        sensor_data.append(sensor)

    current_date += datetime.timedelta(minutes=1)

# Convert lists to Spark Dataframes if needed
production_lines_df = spark.createDataFrame(production_lines)
sensor_data_df = spark.createDataFrame(sensor_data)
