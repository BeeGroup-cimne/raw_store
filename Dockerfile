FROM python:3.10-slim-bookworm
RUN apt-get update
RUN apt-get install -y git
RUN apt-get install -y gcc
RUN apt-get install -y g++
WORKDIR /app
ADD . .
RUN pip install -r requirements.txt
