FROM python:3.10-slim-bookworm
RUN apt-get update
RUN apt-get install -y git
RUN apt-get install -y gcc
RUN apt-get install -y g++
WORKDIR /app
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
COPY . .

#  docker buildx build --platform linux/amd64,linux/arm64 --push -t 1l41bgc7.c1.gra9.container-registry.ovh.net/library/hbase_raw_ingestor:new_kafka .
#  docker buildx build --platform linux/amd64,linux/arm64 --push -t 853583158095.dkr.ecr.eu-west-1.amazonaws.com/ecr-library-hbase-raw-ingestor:prod .
#  TAG=$(git rev-parse --short HEAD)
#  docker buildx build --platform linux/amd64,linux/arm64 --push -t 853583158095.dkr.ecr.eu-west-1.amazonaws.com/ecr-library-hbase-raw-ingestor:prod -t 853583158095.dkr.ecr.eu-west-1.amazonaws.com/ecr-library-hbase-raw-ingestor:$TAG .

#  kubectl set image deployment/raw-store-infraestructures-prod raw-store-infraestructures-prod=853583158095.dkr.ecr.eu-west-1.amazonaws.com/ecr-library-hbase-raw-ingestor:$TAG -n icat-prod-ingestors
