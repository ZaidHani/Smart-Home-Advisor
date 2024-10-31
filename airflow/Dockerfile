FROM apache/airflow:2.10.3rc1-python3.11
COPY requirements.txt /requirements.txt
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r /requirements.txt