FROM apache/spark:3.4.2

USER 0

RUN apt-get update && apt-get install -y \
    curl \
    python3-pip

RUN pip3 install numpy
ENV SPARK_OPTS="--conf spark.driver.extraJavaOptions=-Divy.cache.dir=/tmp -Divy.home=/tmp"

# Creare una directory di lavoro e assegnare i permessi
RUN mkdir -p /home/spark && chown -R spark:spark /home/spark && chmod -R 755 /home/spark

# Impostare i permessi sulla directory /tmp
RUN chmod -R 777 /tmp

# Copiare il codice Python nella directory di lavoro
COPY ./model/TrafficEvents_Aug16_Dec20_Publish.csv /home/spark/TrafficEvents_Aug16_Dec20_Publish.csv

COPY ./model/regressore.py /home/spark/regressore.py

WORKDIR /home/spark

# Impostare l'utente per l'esecuzione
USER spark


