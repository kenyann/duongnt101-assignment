# FROM python:3.9.19-bullseye

FROM apache/airflow:2.9.1-python3.9
USER root
RUN apt update \
    && apt upgrade -y \
    && apt install -y wget \
    && wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb \
    && apt-get install -y ./google-chrome-stable_current_amd64.deb

WORKDIR /opt/airflow/


COPY . .

RUN pip3 install --no-cache-dir -r requirements.txt

CMD python3 main.py