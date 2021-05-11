// Databricks notebook source
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// COMMAND ----------

// MAGIC %md
// MAGIC Leer los datos de los libros

// COMMAND ----------

val books_raw = spark.read.format("csv")
.option("header", "true")
.option("inferSchema", "true")
.option("quote", "\"")
.option("escape", "\"")
.option("multiLine", "true")
.load("dbfs:/FileStore/books/book*.csv")

// COMMAND ----------

// MAGIC %md
// MAGIC Tratar los datos y corregirlos

// COMMAND ----------

val books_raw2 = books_raw
.withColumn("RatingDist5", regexp_extract(books_raw("RatingDist5"), """5:(\d+)""", 1))
.withColumn("RatingDist4", regexp_extract(books_raw("RatingDist4"), """4:(\d+)""", 1))
.withColumn("RatingDist3", regexp_extract(books_raw("RatingDist3"), """3:(\d+)""", 1))
.withColumn("RatingDist2", regexp_extract(books_raw("RatingDist2"), """2:(\d+)""", 1))
.withColumn("RatingDist1", regexp_extract(books_raw("RatingDist1"), """1:(\d+)""", 1))
.withColumn("RatingDistTotal", regexp_extract(books_raw("RatingDistTotal"), """total:(\d+)""", 1))

val books = books_raw2.select(books_raw2("Id").cast(IntegerType).as("Id"),
                            books_raw2("Name"),
                            books_raw2("Authors"),
                            books_raw2("ISBN"),
                            books_raw2("Rating").cast(DoubleType).as("Rating"),
                            books_raw2("PublishYear").cast(IntegerType).as("PublishYear"),
                            books_raw2("PublishMonth").cast(IntegerType).as("PublishMonth"),
                            books_raw2("PublishDay").cast(IntegerType).as("PublishDay"),
                            books_raw2("Publisher"),
                            books_raw2("RatingDist5").cast(IntegerType).as("RatingDist5"),
                            books_raw2("RatingDist4").cast(IntegerType).as("RatingDist4"),
                            books_raw2("RatingDist3").cast(IntegerType).as("RatingDist3"),
                            books_raw2("RatingDist2").cast(IntegerType).as("RatingDist2"),
                            books_raw2("RatingDist1").cast(IntegerType).as("RatingDist1"),
                            books_raw2("RatingDistTotal").cast(IntegerType).as("RatingDistTotal"),
                            books_raw2("CountsOfReview").cast(IntegerType).as("CountsOfReview"),
                            books_raw2("Language"),
                            books_raw2("PagesNumber").cast(IntegerType).as("PagesNumber"),
                            books_raw2("Description")
                            )

// COMMAND ----------

// MAGIC %md
// MAGIC Eliminamos aquellos casos en los que la Id sea nula

// COMMAND ----------

// val books = books_raw3.na.drop()

// COMMAND ----------

// MAGIC %md
// MAGIC Leer los datos de los ratings

// COMMAND ----------

val ratings = spark.read.format("csv")
.option("header", "true")
.option("inferSchema", "true")
.option("quote", "\"")
.option("escape", "\"")
.load("dbfs:/FileStore/books/user*.csv")

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 1. Rating promedio de todos los libros.

// COMMAND ----------

books.select(round(avg("Rating"), 2).alias("Rating Promedio")).show()

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 2. Rating promedio de los libros por autor.

// COMMAND ----------

books
.groupBy(books("Publisher"))
.agg(round(avg("Rating"), 2).alias("Rating Promedio"))
.show()

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 3. Rating promedio de los libros por publisher.

// COMMAND ----------

books
.groupBy(books("Authors"))
.agg(round(avg("Rating"), 2).alias("Rating Promedio"))
.show()

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 4. Número promedio de páginas de todos los libros.

// COMMAND ----------

books.select(round(avg("PagesNumber"), 2).alias("Numero Paginas Promedio")).show()

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 5. Número promedio de páginas de todos los libros por autor.

// COMMAND ----------

books
.groupBy(books("Authors"))
.agg(round(avg("PagesNumber"), 2).alias("Numero Paginas Promedio"))
.show()

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 6. Número promedio de libros publicados por autor.

// COMMAND ----------

books.groupBy(books("Authors"))
.count()
.show()

// COMMAND ----------

books.groupBy(books("Authors"))
.count()
.agg(avg("count"))
.show()

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 7. Ordenar los libros de mayor a menor por número de ratings dados por usuarios.  
// MAGIC Mostrar solo el top15 y excluir aquellos valores sin rating.

// COMMAND ----------

ratings.groupBy("Name")
.count()
.filter(ratings("Name") =!= "Rating")
.select("Name", "count")
.orderBy(desc("count"))
.show(15, false)

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 8. Obtener top5 de ratings más frecuentes otorgados por usuarios.

// COMMAND ----------

ratings.select("Rating")
.groupBy("Rating")
.count()
.orderBy(desc("count"))
.show(5)

// COMMAND ----------

books.select("Id", "Name").filter(books("Id") === 13).show(false)
