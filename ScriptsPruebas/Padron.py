import sys
import findspark
import pyspark
from pyspark.sql import SparkSession, Row

# Iniciar sesión
spark = (SparkSession
        .builder
        .appName("Padron")
        .getOrCreate())

# Imports
from pyspark.sql.functions import trim, col

# Importar datos
ruta = sys.args(0)

padron_df_raw = (spark.read.format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .option("delimiter", ";")
                .option("quotes", "\"")
                .option("emptyValue", 0)
                .load(ruta))

# Limpiar datos
padron_df = (padron_df_raw.select([(trim(i[0])).alias(i[0]) if i[1] == "string" else i[0] for i in padron_df_raw.select("*").dtypes]))

# Incorporar una nueva columna con los habitantes totales
padron_totales = (padron_df
                .withColumn("Totales", col("EspanolesHombres" + col("EspanolesMujeres") + col("ExtranjerosHombres") + col("ExtranjerosMujeres"))))

# Ordenaciones y agrupaciones
padron_total = (padron_totales
                .select(col("Desc_Distrito").alias("Distrito"), col("Desc_Distrito").alias("Barrio"))
                .groupBy("Distrito", "Barrio"))
                
# Particionar por distrito y barrio
padron_part = (padron_totales
                .repartition(col("Distrito"), col("Barrio")))

# Guardar en formato parquet
padron_part.write.partitionedBy("Distrito", "Barrio").format("parquet").mode("overwrite").save(sys.args(1)+"/PadronParquet")

# Guardar en formato json
padron_part.write.partitionedBy("Distrito", "Barrio").format("json").mode("overwrite").save(sys.args(1)+"/PadronJson")