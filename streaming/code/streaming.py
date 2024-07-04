from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, ArrayType, DoubleType
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import from_json, col, create_map, lit
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.conf import SparkConf
from pyspark.sql.functions import from_json, col
from pyspark.ml.classification import MultilayerPerceptronClassificationModel
from pyspark.ml.feature import StringIndexerModel
from pyspark.sql.functions import from_unixtime, unix_timestamp, hour, minute
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import StringIndexerModel, VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, minute, from_unixtime, unix_timestamp, udf
from pyspark.sql.types import StringType
from pyspark.ml.classification import MultilayerPerceptronClassificationModel
from pyspark.ml.feature import StringIndexerModel, VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, unix_timestamp, hour, minute, udf, concat_ws
from pyspark.sql.types import StringType, DoubleType
from pyspark.ml.classification import MultilayerPerceptronClassificationModel
from pyspark.ml.feature import StringIndexerModel, VectorAssembler
from pyspark.sql.functions import struct
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, concat_ws, array


# Configurazione Spark
sparkconf = SparkConf()\
    .set("es.nodes", "elasticsearch") \
    .set("es.port", "9200")

spark = SparkSession.builder.appName("kafkatospark").config(conf=sparkconf).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Kafka e ElasticSearch Config
kafkaServer = "broker:9092"
topic = "pipe"
elastic_index = "incidenti"

# Define the schema for the incidents array
incidents_schema = StructType([
    StructField("type", StringType(), True),
    StructField("geometry", StructType([
        StructField("type", StringType(), True),
        StructField("coordinates", ArrayType(ArrayType(DoubleType())), True)
    ]), True),
    StructField("properties", StructType([
        StructField("events", ArrayType(StructType([
            StructField("code", IntegerType(), True),
            StructField("description", StringType(), True),
            StructField("iconCategory", IntegerType(), True)
        ])), True),
        StructField("startTime", StringType(), True),
        StructField("probabilityOfOccurrence", StringType(), True),
        StructField("endTime", StringType(), True),
        StructField("magnitudeOfDelay", IntegerType(), True),
        StructField("timeValidity", StringType(), True),
        StructField("roadNumbers", ArrayType(StringType()), True),
        StructField("tmc", StructType([
            StructField("tableVersion", StringType(), True),
            StructField("countryCode", StringType(), True),
            StructField("tableNumber", StringType(), True),
            StructField("direction", StringType(), True),
            StructField("points", ArrayType(StructType([
                StructField("offset", IntegerType(), True),
                StructField("location", IntegerType(), True)
            ])), True)
        ]), True),
        StructField("length", DoubleType(), True),
        StructField("lastReportTime", StringType(), True),
        StructField("numberOfReports", StringType(), True),
        StructField("id", StringType(), True),
        StructField("delay", IntegerType(), True),
        StructField("iconCategory", IntegerType(), True),
        StructField("to", StringType(), True),
        StructField("from", StringType(), True)
    ]), True)
])

# Define the main schema
schema = StructType([
    StructField("event", StructType([
        StructField("original", StringType(), True)
    ]), True),
    StructField("incidents", incidents_schema, True),
    StructField("@version", StringType(), True),
    StructField("@timestamp", StringType(), True)
])

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafkaServer) \
    .option("subscribe", topic) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# Trasformazione del dataframe
df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", schema).alias("data"))

print("Schema dopo from_json:")
df.printSchema()

# Select specific fields from the nested structure
df = df.select("data.incidents.geometry.*", "data.incidents.properties.*")

# Aggiungo la colonna location per visualizzare i punti su kibana
df = df.withColumn("location", struct(col("coordinates")[0][1].alias("lat"), col("coordinates")[0][0].alias("lon")))
df = df.withColumn("coordinates", array(col("coordinates")[0][0], col("coordinates")[0][1]))

print("Schema dopo select:")
df.printSchema()

# Filtro le righe con valori null nella colonna id
df = df.filter(df["id"].isNotNull())
df = df.filter(df["magnitudeOfDelay"].isNotNull())
df = df.filter(df["startTime"].isNotNull())
df = df.filter(df["endTime"].isNotNull())
df = df.filter(df["length"].isNotNull())

# Debug print
print("Schema dopo il filtro id non nulli:")
df.printSchema()

df_mlp = df.select("id", "magnitudeOfDelay", "startTime", "endTime", "length")

# Debug print per verificare i tipi di dati
print("Schema df_mlp prima della conversione e riempimento dei nulli:")
df_mlp.printSchema()

df_mlp = df_mlp.withColumn("magnitudeOfDelay", col("magnitudeOfDelay").cast(IntegerType()))
df_mlp = df_mlp.withColumn("length", col("length").cast(DoubleType()))

print("Schema df_mlp dopo la conversione e riempimento dei nulli:")
df_mlp.printSchema()

# Extract hour and minute from startTime and endTime
df_mlp = df_mlp.withColumn("StartHour", hour(from_unixtime(unix_timestamp("startTime", "yyyy-MM-dd'T'HH:mm:ss'Z'"))).cast(IntegerType())) \
               .withColumn("EndHour", hour(from_unixtime(unix_timestamp("endTime", "yyyy-MM-dd'T'HH:mm:ss'Z'"))).cast(IntegerType())) \
               .withColumn("StartMinute", minute(from_unixtime(unix_timestamp("startTime", "yyyy-MM-dd'T'HH:mm:ss'Z'"))).cast(IntegerType())) \
               .withColumn("EndMinute", minute(from_unixtime(unix_timestamp("endTime", "yyyy-MM-dd'T'HH:mm:ss'Z'"))).cast(IntegerType()))

# Debug print per verificare l'estrazione delle ore e dei minuti
print("Schema dopo l'estrazione delle ore e dei minuti:")
df_mlp.printSchema()

#features per il modello
assembler = VectorAssembler(
    inputCols=["magnitudeOfDelay", "StartHour", "StartMinute", "EndHour", "EndMinute", "length"],
    outputCol="features"
)
df_mlp = assembler.transform(df_mlp).select("id", "features")

# verifico il risultato del VectorAssembler
print("Schema dopo il VectorAssembler:")
df_mlp.printSchema()

# carico il modello e le etichette 
mlp_model_path = "/tmp/spark_mlp_modelv"
mlp_model = MultilayerPerceptronClassificationModel.load(mlp_model_path)

indexer_model_path = "/tmp/string_indexer_model"
indexer_model = StringIndexerModel.load(indexer_model_path)

# Perform inference
predictions = mlp_model.transform(df_mlp)

# Debug print per verificare il risultato delle predizioni
print("Schema dopo le predizioni:")
predictions.printSchema()

#Mappa i valori per 
labels = indexer_model.labels
class_mapping = {float(i): label for i, label in enumerate(labels)}

def map_class_to_category(class_index):
    return class_mapping.get(class_index, 'unknown')

map_class_to_category_udf = udf(map_class_to_category, StringType())

predictions = predictions.withColumn("category", map_class_to_category_udf(col("prediction")))

# Debug print per verificare il mapping delle categorie
print("Schema dopo il mapping delle categorie:")
predictions.printSchema()

# Join predizioni con il dataframe originale
df_final = df.join(predictions.select("id", "category"), on="id")

# Debug print per verificare il risultato del join con le predizioni
print("Schema dopo il join con le predizioni:")
df_final.printSchema()

# Select all columns
df_final_selected = df_final.select(
    "id", 
    "type", 
    "coordinates", 
    "events", 
    "startTime", 
    "probabilityOfOccurrence", 
    "endTime", 
    "magnitudeOfDelay", 
    "timeValidity", 
    "roadNumbers", 
    "tmc.tableVersion", 
    "tmc.countryCode", 
    "tmc.tableNumber", 
    "tmc.direction", 
    "tmc.points", 
    "length", 
    "lastReportTime", 
    "numberOfReports", 
    "delay", 
    "iconCategory", 
    "to", 
    "from", 
    "category",
    "location"
)


# Write the result to Elasticsearch
query = df_final_selected.writeStream \
    .format("org.elasticsearch.spark.sql") \
    .option("es.resource", "incidenti") \
    .option("checkpointLocation", "/tmp/checkpoints") \
    .option("es.nodes", "elasticsearch") \
    .option("es.port", "9200") \
    .start("incidenti") \
    .awaitTermination()