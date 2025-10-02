FROM apache/airflow:2.10.2

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
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow

# Copy requirements file
COPY requirements.txt /requirements.txt

# Install Python packages
RUN pip install --no-cache-dir -r /requirements.txt

# Ensure redis is installed (remove from previous line, add here if needed)
RUN pip install --no-cache-dir redis>=4.5.4