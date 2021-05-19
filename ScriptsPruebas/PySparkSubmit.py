import sys
import findspark
import pyspark
from pyspark.sql import SparkSession

# Iniciar sesión
spark = (SparkSession
        .builder
        .appName("PySparkLecturaEscritura")
        .getOrCreate)

# Imports
from pyspark.sql.functions import trim

# Importar datos
ruta = "hdfs://192.168.10.62:9000/carlos/datos/Rango_Edades_Seccion_202012.csv"

padron_df_raw = (spark.read.format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .option("delimiter", ";")
                .option("quotes", "\"")
                .option("emptyValue", 0)
                .load(ruta))

# Limpiar datos
padron_df = (padron_df_raw.select([(trim(i[0])).alias(i[0]) if i[1] == "string" else i[0] for i in padron_df_raw.select("*").dtypes]))

# Crear view temporal y guardarla como tabla
padron_df.createOrReplaceTempView("padronSpark")

spark.sql("CREATE TABLE padron.padronSpark as SELECT * FROM padronSpark")

# Leer los datos de la tabla
spark.sql("SELECT * FROM padron.padronSpark")