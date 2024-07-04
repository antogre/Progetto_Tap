from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, minute, from_unixtime, unix_timestamp, udf, count
from pyspark.sql.types import StringType
from pyspark.ml.classification import MultilayerPerceptronClassificationModel
from pyspark.ml.feature import StringIndexerModel, VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, monotonically_increasing_id, hour, minute, expr
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import MultilayerPerceptronClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

import os

def to_vector_udf(features):
    if features and isinstance(features[0], list):
        flat_list = [item for sublist in features for item in sublist]
        return Vectors.dense([float(x) for x in flat_list])
    else:
        return Vectors.dense([float(x) for x in features])

to_vector = udf(to_vector_udf, VectorUDT())

spark = SparkSession.builder.appName("MLP with Spark NLP").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("Caricamento del dataset...")
df = spark.read.csv("./TrafficEvents_Aug16_Dec20_Publish.csv", header=True, inferSchema=True).limit(700000)
df = df.drop('EventId',	'TMC',	'Description',	'TimeZone',	'LocationLat',	'LocationLng',	'AirportCode	Number',	'Street',	'Side',	'City',	'County',	'State',	'ZipCode')

valid_categories = ['Congestion', 'Flow-Incident', 'Accident', 'Lane-Blocked','Broken-Vehicle', 'Construction', 'Event']
df = df.filter(col("Type").isin(valid_categories))

df = df.withColumn("id", monotonically_increasing_id())

print("Utilizzo di un campione casuale del dataset...")
df = df.sample(withReplacement=False, fraction=0.40, seed=42) 

df = df.withColumn("StartHour", hour(col("StartTime(UTC)"))) \
       .withColumn("StartMinute", minute(col("StartTime(UTC)"))) \
       .withColumn("EndHour", hour(col("EndTime(UTC)"))) \
       .withColumn("EndMinute", minute(col("EndTime(UTC)")))

df = df.withColumn("DistanceMeters", col("Distance(mi)") * 1609.34)

print("Indicizzazione delle categorie...")
stringIndexer = StringIndexer(inputCol="Type", outputCol="label", handleInvalid="keep")
indexer_model = stringIndexer.fit(df)
df_indexed = indexer_model.transform(df)

indexer_model_save_path = "/tmp/string_indexer_model"
os.makedirs(os.path.dirname(indexer_model_save_path), exist_ok=True)
indexer_model.write().overwrite().save(indexer_model_save_path)

print("Controllo delle etichette generate...")
df_indexed.groupBy("label").count().show()
df_indexed.select("Type", "label").distinct().show()

print("Bilanciamento del dataset per undersampling...")
min_count = df_indexed.groupBy("label").count().selectExpr("MIN(count)").first()[0]
df_balanced = df_indexed.sampleBy("label", fractions={x: min_count / count for x, count in df_indexed.groupBy("label").count().rdd.collectAsMap().items()})
df_balanced.select("Type", "label").distinct().show()

print("Preparazione dei dati per Spark MLlib...")
assembler = VectorAssembler(
    inputCols=["Severity", "StartHour", "StartMinute", "EndHour", "EndMinute", "DistanceMeters"],
    outputCol="features"
)
df_ml = assembler.transform(df_balanced).select("id", "features", "label")

# Controlla le etichette prima dell'addestramento
print("Controllo delle etichette prima dell'addestramento...")
df_ml.groupBy("label").count().show()

print("Addestramento del modello MLP...")
layers = [6, 32, 64, 128, 64, 32, 7]  # Number of input features, hidden layers, and number of categories
mlp = MultilayerPerceptronClassifier(maxIter=20, layers=layers, blockSize=128, seed=42)
train, test = df_ml.randomSplit([0.7, 0.3], seed=42)
model = mlp.fit(train)

print("Valutazione del modello...")
result = model.transform(test)
evaluator = MulticlassClassificationEvaluator(metricName="accuracy")
accuracy = evaluator.evaluate(result)
print(f"Accuracy: {accuracy}")

print("Salvataggio del modello...")
model_save_path = "/tmp/mod_mlp/spark_mlp_modelv"
os.makedirs(os.path.dirname(model_save_path), exist_ok=True)
model.write().overwrite().save(model_save_path)

result_with_text = result.join(df.select("id", "Type"), on="id")

print("Risultati:")
result_with_text.select("label", "prediction").show(truncate=False)


print("Numero di predizioni per categoria:")
result_with_text.groupBy("prediction").count().show(truncate=False)

print("Confronto tra categorie reali e predette:")
result_with_text.groupBy("Type", "prediction").count().show(truncate=False)