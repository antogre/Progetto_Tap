## Analisi del Traffico in Tempo Reale a Milano

# Introduzione
Per analizzare i dati  del Traffico in Tempo Reale a Milano è stato progettato un sistema per raccogliere, elaborare, analizzare e visualizzare i dati  in tempo reale . Il progetto utilizza Docker, Kafka, Spark, Elasticsearch e Kibana per creare una soluzione scalabile ed efficiente per il monitoraggio e l'analisi del traffico.

# Struttura del Progetto
Il progetto è organizzato in più container Docker che interagiscono tra loro per implementare il flusso dei dati. Di seguito sono elencati i principali componenti e i passaggi preliminari necessari per configurare il sistema.

## Requisiti
Docker

Docker Compose

## Clonare il Repository
Per prima cosa, clona il repository del progetto:

```bash
git clone git@github.com:antogre/Progetto_Tap.git
cd Progetto_Tap
```
## Passaggi Preliminari
Prima di avviare il sistema, è necessario costruire alcune immagini Docker e configurare i container. Segui questi passaggi per configurare l'ambiente.

# 1. Costruire le Immagini Docker

# Spark
```bash
docker build -t sparkrc -f streaming/Dockerfile .
```

# 2. Avviare il Sistema
Una volta completati i passaggi preliminari, avvia il sistema utilizzando Docker Compose.

```bash
docker-compose up -d
```

# 3. Arrestare il Sistema
Per arrestare il sistema, utilizza il seguente comando:

```bash
docker-compose down
```

## Componenti del Sistema

Logstash: Ingestione dei dati e invio a Kafka.

Zookeeper: Servizio di coordinamento per Kafka.

Kafka: Message broker per il trasferimento dei dati.

Spark: Elaborazione dei dati in tempo reale.

Elasticsearch: Archiviazione e ricerca dei dati.

Kibana: Visualizzazione dei dati in tempo reale.

## Utilizzo del Sistema
Accedi ai componenti del sistema tramite le seguenti porte:

Kibana: http://localhost:5601

Kafka Ui: http://localhost:8080

Elasticsearch: http://localhost:9200
## Conclusione
Il sistema sfrutta tecnologie avanzate per fornire una soluzione robusta e scalabile per il monitoraggio e l'analisi del traffico a Milano. Seguendo i passaggi preliminari e utilizzando Docker Compose, puoi facilmente configurare ed eseguire il sistema.
