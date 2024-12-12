FROM apache/airflow:2.5.0
ENV TZ="Asia/Jakarta"
COPY airflow.cfg /opt/airflow/
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt
