# Ejercicios Spark

Serie de diversos ejercicios realizados en Spark.
## ScriptsPruebas:
#### Rango_Edades_Seccion_202012.csv
* Fichero csv con datos del padrón Madrid.
#### upload_file.sh:  
* Sube el fichero **Rango_Edades_Seccion_202012.csv** a **HDFS**.
#### PySparkLecturaEscritura.py:  
* Lee el archivo csv en **HDFS** y lo guarda en formato parquet y json en **HDFS**.
#### PySparkHive.py:  
* Lee el archivo parquet en **HDFS** y lo guarda como una tabla Hive.
#### ScalaSparkLecturaEscritura.scala:  
* Lee un archivo csv en **HDFS** y lo guarda en formato parquet y json en **HDFS**.
#### ScalaSparkHive.scala:  
* Lee el archivo parquet en **HDFS** y lo guarda como una tabla Hive.
#### PySparkSubmit.py:  
* Lee un archivo csv en **HDFS** y lo guarda como una tabla Hive.
#### ScalaSparkSubmit.scala:  
* Lee un archivo csv en **HDFS** y lo guarda como una tabla Hive.
#### ScalaSparkSubmit.jar
* Comprimido de **ScalaSparkSubmit.scala**.
#### final.sh:  
* Lanza **upload_file.sh**.  
* Lanza **PySparkLecturaEscritura.py** y **PySparkHive.py** en PySpark.  
* Lanza **ScalaSparkLecturaEscritura.scala** y **ScalaSparkHive.scala** en scala-shell.  
* Lanza **PySparkSubmit.py** con spark-submit.
* Lanza **ScalaSparkSubmit.jar** con spark-submit.

## Books:
Plataforma: Databricks (Scala Spark).
* Lectura y corrección de datos cargados desde varios archivos csv.
* Obtención de diferentes promedios y conteos filtrando y agrupando datos.
## DelayFights:
Plataforma: Jupyter Notebooks (PySpark).
* Tratamiento de datos que incluyen fechas y horas.
## Nasa log:
Plataforma: Databricks (Scala Spark).
* Lectura y corrección de datos cargados desde varios logs.
* Expresiones regulares.
* Tratamiento de los datos con selecciones, agrupaciones, conteos y filtrados.
## Padron & Practica Padron:
Plataforma: Databricks (Scala Spark) y Jupyter Notebooks (PySpark).
* Lectura y corrección de datos cargados desde un csv.
* Selecciones, agrupaciones, conteos, filtrados...
* Vistas temporales.
* Añadir y quitar columnas.
* Particionamiento.
* Caché.
* Funciones de ventana.
* Función Pivot.
