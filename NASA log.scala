// Databricks notebook source
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// COMMAND ----------

// MAGIC %md
// MAGIC Queremos leer un archivo con los logs de la NASA

// COMMAND ----------

val nasalog_csv = spark.read.text("dbfs:/FileStore/tables/access_log_*95")

// COMMAND ----------

// MAGIC %md
// MAGIC Los datos no están organizados en ningún schema así que los organizaremos nosotros usando expresiones regulares

// COMMAND ----------

val regex = """(\S+)\s(-|\S+)\s(-|\S+)\s\[(\S+)\s(\S+)]\s\"(GET|POST|PUT|TRACE|HEAD)?\s*(\S+|\S+\s*\S*)\s*(HTTP\S+\s*\S*)?\"\s(\d{3})\s(-|\d+)"""

val parsed_df = nasalog_csv.select(regexp_extract($"value", regex, 1).alias("host"),
                                   regexp_extract($"value", regex, 2).alias("user"),
                                   regexp_extract($"value", regex, 3).alias("id"),
                                   regexp_extract($"value", regex, 4).alias("date"),
                                   regexp_extract($"value", regex, 5).alias("timezone"),
                                   regexp_extract($"value", regex, 6).alias("method"),
                                   regexp_extract($"value", regex, 7).alias("file"),
                                   regexp_extract($"value", regex, 8).alias("protocol"),
                                   regexp_extract($"value", regex, 9).cast("integer").alias("status"),
                                   regexp_extract($"value", regex, 10).cast("integer").alias("size"))

parsed_df.show()

// COMMAND ----------

val host = """(\S+)"""
val userid = """\s(-|\S+)\s(-|\S+)"""
val date = """\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2})"""
val timezone = """(-\d{4})]"""
val methodfileprotocol = """\"(GET|POST|PUT|TRACE|HEAD)?\s(-|\S+)\s(\S+)\""""
val status = """\s(\d{3})\s"""
val size = """(-|\d+)$"""

val parsed_df2 = nasalog_csv.select(regexp_extract($"value", host, 1).alias("host"),
                                   regexp_extract($"value", userid, 1).alias("user"),
                                   regexp_extract($"value", userid, 2).alias("id"),
                                   regexp_extract($"value", date, 1).alias("date"),
                                   regexp_extract($"value", timezone, 1).alias("timezone"),
                                   regexp_extract($"value", methodfileprotocol, 1).alias("method"),
                                   regexp_extract($"value", methodfileprotocol, 2).alias("file"),
                                   regexp_extract($"value", methodfileprotocol, 3).alias("protocol"),
                                   regexp_extract($"value", status, 1).cast("integer").alias("status"),
                                   regexp_extract($"value", size, 1).cast("integer").alias("size"))

parsed_df2.show()

// COMMAND ----------

// MAGIC %md
// MAGIC También podríamos hacer el regex con la siguiente expresión.  
// MAGIC ~~~
// MAGIC (\S+)\s(-|\S+)\s(-|\S+)\s\[(\S+)\s\-\d{4}\]\s\"(GET|POST|PUT|TRACE|HEAD)?\s*(\S+|\S+\s*\S*)\s*(HTTP\S+\s*\S*)?\"\s(\d{3})\s(-|\d+)
// MAGIC ~~~

// COMMAND ----------

val parsed_df3 = parsed_df2
    .withColumn("size", when($"size".isNull, 0).otherwise($"size"))
parsed_df3.show()

// COMMAND ----------

// MAGIC %md
// MAGIC Volvemos a guardar los logs, ahora en un parquet y con su esquema y datos organizados

// COMMAND ----------

parsed_df3.write
.format("parquet")
.mode("overwrite")
.save("/nasalog")

// COMMAND ----------

// MAGIC %md
// MAGIC Leemos el archivo parquet para tratar sus datos

// COMMAND ----------

val nasalog_df = spark.read.parquet("dbfs:/nasalog/")

// COMMAND ----------

// MAGIC %md
// MAGIC Ahora vamos a agrupar los datos por su protocolo para ver qué distintos protocolos se han utilizado

// COMMAND ----------

nasalog_df.select("protocol")
  .distinct()
  .show()

// COMMAND ----------

// MAGIC %md
// MAGIC Observamos que solo hay 6 opciones diferentes, 2 de ellas siendo las mismas (HTTP/V1.0 y HTTP/1.0), un HTTP/* y otras 3 opciones que puede que sean algún tipo de error.  
// MAGIC Vamos a comprobar cuantas veces sale cada una de estas distintas opciones.

// COMMAND ----------

nasalog_df.select("protocol")
  .groupBy("protocol")
  .count()
  .orderBy(desc("count"))
  .show()

// COMMAND ----------

nasalog_df
.filter(nasalog_df("protocol") === "")
.show()

// COMMAND ----------

// MAGIC %md
// MAGIC Vamos a mostrar la fila con el protocolo "a" y "STS-69\</a>\<p>", a ver qué ha pasado en ellas

// COMMAND ----------

nasalog_df.filter(nasalog_df("protocol") === "a" || nasalog_df("protocol") === "STS-69</a><p>")
.show()

// COMMAND ----------

// MAGIC %md
// MAGIC Observamos que todas ellas devuelven un error 404.   
// MAGIC Estaría bien comprobar qué ha pasado en aquellos logs en los que el protocolo está vacío.

// COMMAND ----------

nasalog_df.select("status")
.filter(nasalog_df("protocol") === "")
.groupBy("status")
.count()
.orderBy("status")
.show()

// COMMAND ----------

// MAGIC %md
// MAGIC Aparentemente no hay ningún tipo de patrón entre por qué no hay ningún protocolo y el status.  
// MAGIC Vamos a unir los 2 HTTP/1.0 y las otras 2 opciones y el vacío vamos a guardarlas como "NA"

// COMMAND ----------

val nasalog_df2 = nasalog_df.withColumn("protocol",
                                       when(nasalog_df("protocol") === "HTTP/V1.0", "HTTP/1.0")
                                       .when(nasalog_df("protocol") === "HTTP/1.0", "HTTP/1.0")
                                       .when(nasalog_df("protocol") === "HTTP/*", "HTTP/*")
                                       .otherwise("NA"))
nasalog_df2.select("protocol").distinct().show()

// COMMAND ----------

// MAGIC %md
// MAGIC Ahora ya tenemos los datos de los diferentes protocolos guardados correctamente.  
// MAGIC Vamos a seguir comprobando los diferentes tipos de datos que hemos almacenado, ahora mirando cuántos métodos hay.

// COMMAND ----------

nasalog_df2.select("method")
.groupBy("method")
.count()
.orderBy(desc("count"))
.show(false)

// COMMAND ----------

// MAGIC %md
// MAGIC Vamos a modificar la fila para que en vez de un espacio en blanco ponga "NA".

// COMMAND ----------

val nn = !(nasalog_df2("method") === "GET") && !(nasalog_df2("method") === "HEAD") && !(nasalog_df2("method") === "POST")

val nasalog_df3 = nasalog_df2
.withColumn("method", when(nn, "NA").otherwise($"method"))
nasalog_df3.select("method").distinct().show()

// COMMAND ----------

// MAGIC %md
// MAGIC Ya tenemos los registros de los protocolos corregidos.  
// MAGIC Ahora vamos a comprobar los diferentes status, cuáles hay y cuántas veces salen.

// COMMAND ----------

nasalog_df3.select("status")
.groupBy("status")
.count()
.orderBy(desc("count"))
.show()

// COMMAND ----------

// MAGIC %md
// MAGIC Podemos comprobar que en este apartado todo se ve normal y no hay ningún tipo de error.  
// MAGIC Ahora vamos a comprobar cuál es el fichero más grande que se ha enviado.

// COMMAND ----------

nasalog_df3.orderBy(desc("size")).limit(1).show()

// COMMAND ----------

// MAGIC %md
// MAGIC Ahora vamos a buscar cuál ha sido el fichero más solicitado.

// COMMAND ----------

nasalog_df3.groupBy("file")
.count()
.orderBy(desc("count")).show()

// COMMAND ----------

// MAGIC %md
// MAGIC Y ahora por ejemplo vamos a buscar el host más frecuente.

// COMMAND ----------

nasalog_df3.groupBy("host")
.count()
.orderBy(desc("count"))
.show()

// COMMAND ----------

// MAGIC %md
// MAGIC Ahora para tratar con fechas vamos a convertir el date a un formato de fechas en vez de string para poder tratarlo correctamente.

// COMMAND ----------

val nasalog_df5 = (nasalog_df3
      .withColumn("date", to_timestamp(nasalog_df3("date"), "dd/MMM/yyyy:HH:mm:ss")))
nasalog_df5.printSchema()
nasalog_df5.show()

// COMMAND ----------

// MAGIC %md
// MAGIC Comprobemos por ejemplo cuánto tráfico hubo cada día de cada mes

// COMMAND ----------

nasalog_df5.withColumn("dia", dayofmonth(nasalog_df5("date")))
.withColumn("mes", month(nasalog_df5("date")))
.groupBy("dia", "mes")
.count()
.orderBy(desc("count"))
.show()

// COMMAND ----------

// MAGIC %md
// MAGIC Podríamos ver cuál es el fichero más grande que se ha enviado cada día

// COMMAND ----------

nasalog_df5.withColumn("dia", dayofmonth(nasalog_df5("date")))
.withColumn("mes", month(nasalog_df5("date")))
.groupBy("dia", "mes")
.agg(max("size").alias("maximo"))
.orderBy(desc("maximo"))
.show()

// COMMAND ----------

// MAGIC %md
// MAGIC Todos los errores 400 tienen la parte de method file y protocol corrupta y no se muestra correctamente.

// COMMAND ----------

nasalog_df.filter(nasalog_df("status") === "400").show()

// COMMAND ----------

nasalog_df.filter(nasalog_df("date") === "31/Aug/1995:11:44:19").show()


// COMMAND ----------

// MAGIC %md
// MAGIC Obtener el intervalo de horas en el que más conexiones hay?

// COMMAND ----------

nasalog_df5.withColumn("hora", hour(nasalog_df5("date")))
.groupBy("hora")
.count()
.orderBy(desc("count"))
.show()

// COMMAND ----------

// MAGIC %md
// MAGIC Comprobemos cuantos errores 404 hay cada día.

// COMMAND ----------

nasalog_df5
.withColumn("dia", dayofmonth(nasalog_df5("date")))
.filter(nasalog_df5("status") === 404)
.groupBy("dia")
.count()
.orderBy(desc("count"))
.show()

// COMMAND ----------

// MAGIC %md
// MAGIC Vamos a mirar cuantos timezones diferentes hay.

// COMMAND ----------

nasalog_df5.select("timezone").distinct().show()

// COMMAND ----------

// MAGIC %md
// MAGIC Todos los logs son de la misma timezone, lo cuál nos podría indicar que esta información es irrelevante.  
// MAGIC Comprobemos qué diferentes user e ids hay.

// COMMAND ----------

nasalog_df5.select("user", "id").distinct().show()

// COMMAND ----------

// MAGIC %md
// MAGIC Otro campo en el que obtenemos información irrelevante, ya que ninguno de estos campos tiene ningún valor.
