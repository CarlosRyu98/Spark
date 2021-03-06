import sys
import pyspark
from pyspark.sql import SparkSession

# Iniciar sesión
spark = (SparkSession
        .builder
        .appName("PySparkLecturaEscritura")
        .getOrCreate())

# Imports
from pyspark.sql.functions import trim

# Importar datos
ruta = sys.argv[0] + "/datos/Rango_Edades_Seccion_202012.csv"

padron_df_raw = (spark.read.format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .option("delimiter", ";")
                .option("quotes", "\"")
                .option("emptyValue", 0)
                .load(ruta))

# Limpiar datos
padron_df = (padron_df_raw.select([(trim(i[0])).alias(i[0]) if i[1] == "string" else i[0] for i in padron_df_raw.select("*").dtypes]))

# Guardar en formato parquet
padron_df.write.format("parquet").mode("overwrite").save(sys.argv[1] + "/outPy/PadronParquet")

# Guardar en formato json
padron_df.write.format("json").mode("overwrite").save(sys.argv[1] + "/outPy/PadronJson")