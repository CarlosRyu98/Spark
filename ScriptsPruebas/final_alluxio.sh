#! /bin/bash

# Llamar al primer script para subir los datos a hdfs
# ./upload_file.sh

# Lanzar los scripts de PySpark
pyspark < PySparkLecturaEscritura.py alluxio://alluxio-master:19998/mnt/hdfs/carlos --master spark//spark-master:7077
pyspark < PySparkHive.py alluxio://alluxio-master:19998/mnt/hdfs/carlos --master spark//spark-master:7077

# Lanzar los scripts de Scala Spark
spark-shell -i ./ScalaSparkLecturaEscritura.scala --master spark://spark-master:7077 alluxio://alluxio-master:19998/mnt/hdfs/carlos
spark-shell -i ./ScalaSparkHive.scala --master spark://spark-master:7077 alluxio://alluxio-master:19998/mnt/hdfs/carlos

# Lanzar el spark-submit de python
spark-submit PySparkSubmit.py --master spark://spark-master:7077 --name "PySparkSubmit" alluxio://alluxio-master:19998/mnt/hdfs/carlos

# Lanzar el spark-submit de scala
spark-submit ScalaSparkSubmit-jar-with-dependencies.jar --class ScalaSparkSubmit --master spark://spark-master:7077 --name "ScalaSparkSubmit" alluxio://alluxio-master:19998/mnt/hdfs/carlos
