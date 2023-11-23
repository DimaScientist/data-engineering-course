FROM apache/airflow:2.2.3-python3.9

COPY ./requirements.txt ./
RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt && rm requirements.txt