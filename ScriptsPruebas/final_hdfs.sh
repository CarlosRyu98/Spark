#! /bin/bash

# Llamar al primer script para subir los datos a hdfs
# ./upload_file.sh

# Lanzar los scripts de PySpark
pyspark < PySparkLecturaEscritura.py hdfs://192.168.10.62:9000/carlos --master spark://spark-master:7077
pyspark < PySparkHive.py hdfs://192.168.10.62:9000/carlos --master spark//spark-master:7077

# Lanzar los scripts de Scala Spark
spark-shell -i ./ScalaSparkLecturaEscritura.scala --master spark://spark-master:7077 hdfs://192.168.10.62:9000/carlos
spark-shell -i ./ScalaSparkHive.scala --master spark://spark-master:7077 hdfs://192.168.10.62:9000/carlos

# Lanzar el spark-submit de python
spark-submit PySparkSubmit.py --master spark://spark-master:7077 --name "PySparkSubmit" hdfs://192.168.10.62:9000/carlos

# Lanzar el spark-submit de scala
spark-submit ScalaSparkSubmit-jar-with-dependencies.jar --class ScalaSparkSubmit --master spark://spark-master:7077 --name "ScalaSparkSubmit" hdfs://192.168.10.62:9000/carlos
