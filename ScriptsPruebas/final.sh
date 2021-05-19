#! /bin/bash

# Llamar al primer script para subir los datos a hdfs
./upload_file.sh

# Lanzar los scripts de PySpark
pyspark < PySparkLecturaEscritura.py
pyspark < PySparkHive.py

# Lanzar los scripts de Scala Spark
spark-shell -i ScalaSparkLecturaEscritura.scala
spark-shell -i ScalaSparkHive.scala

# Lanzar el spark-submit de python
spark-submit PySparkSubmit.py --master spark_//spark-master:7077 --name "PySparkSubmit" 

# Lanzar el spark-submit de scala
spark-submit ScalaSparkSubmit-dependencies.jar --class ScalaSparkSubmit --master spark://spark-master:7077 --name "ScalaSparkSubmit"
