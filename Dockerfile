# Use the official Airflow image as the base
FROM apache/airflow:latest

# COPY requirements.txt /

# Install the Docker provider for Airflow
RUN pip install apache-airflow-providers-docker
# RUN pip install --upgrade pip

# RUN pip install --no-cache-dir -r /requirements.txt
