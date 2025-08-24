FROM apache/airflow:3.0.1

USER root
RUN apt-get update && apt-get install -y \
  build-essential \
  libmysqlclient-dev \
  libpq-dev \
  libsasl2-dev \
  unixodbc-dev \
  libffi-dev \
  libssl-dev \
  gcc \
  curl \
  git \
  && apt-get clean

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
