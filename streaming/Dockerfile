FROM apache/spark:3.4.2

USER 0

RUN apt-get update && apt-get install -y \
    curl \
    python3-pip

RUN pip3 install numpy \
    elasticsearch \
    langchain_elasticsearch 

# Installare le librerie Python necessarie
RUN pip3 install numpy 
ENV SPARK_OPTS="--conf spark.driver.extraJavaOptions=-Divy.cache.dir=/tmp -Divy.home=/tmp"

RUN mkdir -p /home/spark && chown -R spark:spark /home/spark && chmod -R 755 /home/spark

RUN chmod -R 777 /tmp

COPY ./streaming/code /home/spark
WORKDIR /home/spark

USER spark
