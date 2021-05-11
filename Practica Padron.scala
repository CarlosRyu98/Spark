// Databricks notebook source
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC A la hora de importar un archivo hay que especificar el formato y se puede usar el comando option para ajustarlo

// COMMAND ----------

// MAGIC %md
// MAGIC #### 6.1 Importar un archivo csv con opciones para quitar comillas, espacios y cambien los valores nulos

// COMMAND ----------

val schema = StructType(
  Array(
    StructField("COD_DISTRITO", IntegerType, true),
    StructField("DESC_DISTRITO", StringType, true),
    StructField("COD_DIST_BARRIO", IntegerType, true),
    StructField("DESC_BARRIO", StringType, true),
    StructField("COD_BARRIO", IntegerType, true),
    StructField("COD_DIST_SECCION", IntegerType, true),
    StructField("COD_SECCION", IntegerType, true),
    StructField("COD_EDAD_INT", IntegerType, true),
    StructField("EspanolesHombres", IntegerType, true),
    StructField("EspanolesMujeres", IntegerType, true),
    StructField("ExtranjerosHombres", IntegerType, true),
    StructField("ExtranjerosMujeres", IntegerType, true)
   )
)

val padron_df_raw = spark.read.format("csv")
.option("header", "true")
.option("inferSchema", "false")
.option("delimiter", ";")
.option("quotes", "\"")
.option("ignoreTrailingWhiteSpace", "true") // No funciona
.option("ignoreLeadingWhiteSpace", "true") // No funciona
.option("emptyValue", 0)
.schema(schema)
.load("dbfs:/FileStore/padron/Rango_Edades_Seccion_202012-1.csv")

padron_df_raw.show(5)

// COMMAND ----------

// MAGIC %md
// MAGIC Utilizamos la siguiente función para borrar los espacios en blanco.

// COMMAND ----------

val padron_df = padron_df_raw.select(schema.fields.map(field => {
  if (field.dataType == StringType) {
   trim(col(field.name)).as(field.name)
  } else {
    col(field.name)
  }
}):_*)

padron_df.show(5)

// COMMAND ----------

// MAGIC %md
// MAGIC #### 6.3 Enumerar todos los barrios diferentes

// COMMAND ----------

// DBTITLE 0,6.3 Enumerar barrios
val barrios = padron_df.select("DESC_BARRIO").distinct()

barrios.show()

// COMMAND ----------

// MAGIC %md
// MAGIC #### 6.4 Crear una vista temporal y contar el número de barrios a través de ella.

// COMMAND ----------

padron_df.createOrReplaceTempView("padron")

sqlContext.sql("select count(distinct DESC_BARRIO) from padron").show

// COMMAND ----------

// MAGIC %md
// MAGIC #### 6.5 Crear una nueva columna que muestre la longitud de la columna _DESC_DISTRITO_ llamada _longitud_.

// COMMAND ----------

val padron_long = padron_df.withColumn("longitud", length(col("DESC_DISTRITO")))
padron_long.show(10)

// COMMAND ----------

// MAGIC %md
// MAGIC #### 6.6 Crear una nueva columna que muestre el valor 5 para cada registro de la tabla.

// COMMAND ----------

val padron_5 = padron_df.withColumn("Extra", lit(5))
padron_5.show()

// COMMAND ----------

// MAGIC %md
// MAGIC #### 6.7 Borrar la columna anterior.

// COMMAND ----------

val padron_drop = padron_5.drop("Extra")
padron_drop.show()

// COMMAND ----------

// MAGIC %md
// MAGIC #### 6.8 Particionar el DataFrame por las variables _DESC_DISTRITO_ y _DESC_BARRIO_.

// COMMAND ----------

val padron_part = padron_df.repartition(padron_df("DESC_DISTRITO"), padron_df("DESC_BARRIO"))

// COMMAND ----------

// MAGIC %md
// MAGIC #### 6.9 Almacenar en caché y comprobar la _UI_.

// COMMAND ----------

padron_part.cache()
padron_part.count()
padron_part.count()

// COMMAND ----------

// MAGIC %md
// MAGIC #### 6.10 Mostrar el total de _EspanolesHombres_, _EspanolesMujeres_, _ExtranjerosHombres_ y _ExtranjerosMujeres_ para cada barrio de cada distrito.

// COMMAND ----------

padron_part.select("DESC_DISTRITO", "DESC_BARRIO", "EspanolesHombres", "EspanolesMujeres", "ExtranjerosHombres", "ExtranjerosMujeres")
.groupBy("DESC_DISTRITO", "DESC_BARRIO")
.sum("EspanolesHombres", "EspanolesMujeres", "ExtranjerosHombres", "ExtranjerosMujeres")
.orderBy(sum("ExtranjerosMujeres"), sum("ExtranjerosHombres"))
.show

// COMMAND ----------

// MAGIC %md
// MAGIC #### 6.11 Eliminar el registro en caché.

// COMMAND ----------

padron_part.unpersist()

// COMMAND ----------

// MAGIC %md
// MAGIC #### 6.12 Crear un DataFrame que muestre únicamente _DESC_BARRIO_, _DESC_DISTRITO_ y el total de _EspanolesHombres_ en cada distrito de cada barrio. Únelo con un join al original.

// COMMAND ----------

val padron_esho = padron_df.select("DESC_BARRIO", "DESC_DISTRITO", "EspanolesHombres")
.groupBy("DESC_BARRIO", "DESC_DISTRITO")
.sum("EspanolesHombres")

padron_esho.join(padron_df, padron_df("DESC_BARRIO") === padron_esho("DESC_BARRIO") && padron_df("DESC_DISTRITO") === padron_esho("DESC_DISTRITO"), "inner")
.select("EspanolesHombres", "sum(EspanolesHombres)")
.show()

// COMMAND ----------

// MAGIC %md
// MAGIC #### 6.13 Repetir utilizando funciones de ventana.

// COMMAND ----------

padron_df.withColumn("Suma", sum("EspanolesHombres") over Window.partitionBy("DESC_BARRIO", "DESC_DISTRITO"))
.select("DESC_BARRIO", "DESC_DISTRITO", "Suma")
.distinct
.show

// COMMAND ----------

// MAGIC %md
// MAGIC #### 6.14 Mostrar la media de EspanolesMujeres para los distritos _CENTRO_, _BARAJAS_ y _RETIRO_ por rango de edad utilizando la función Pivot.

// COMMAND ----------

val padron_pivot = padron_df.groupBy("COD_EDAD_INT")
.pivot("DESC_DISTRITO", Seq("BARAJAS", "CENTRO", "RETIRO"))
.avg("EspanolesMujeres")
.orderBy("COD_EDAD_INT")

padron_pivot.show()

// COMMAND ----------

// MAGIC %md
// MAGIC #### 6.15 Crea 3 columnas en las que se indique qué porcentaje de _EspanolesMujeres_ supone

// COMMAND ----------

val padron_percent = padron_pivot
.withColumn("BARAJAS_PERCENT", round(col("BARAJAS") / (col("BARAJAS") + col("CENTRO") + col("RETIRO")) * 100, 2))
.withColumn("CENTRO_PERCENT", round(col("CENTRO") / (col("BARAJAS") + col("CENTRO") + col("RETIRO")) * 100, 2))
.withColumn("RETIRO_PERCENT", round(col("RETIRO") / (col("BARAJAS") + col("CENTRO") + col("RETIRO")) * 100, 2))
padron_percent.show


// COMMAND ----------

// MAGIC %md
// MAGIC #### 6.16 Guarda el archivo como csv particionado por distrito y por barrio en el directorio local

// COMMAND ----------

padron_part.write
.format("csv")
.mode("overwrite")
.save("/PadronCSV")

// COMMAND ----------

// MAGIC %md
// MAGIC #### 6.17 El mismo guardado pero como parquet.

// COMMAND ----------

padron_part.write
.format("parquet")
.mode("overwrite")
.save("/PadronParquet")
