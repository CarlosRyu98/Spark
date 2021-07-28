import sys
import findspark
import pyspark
from pyspark.sql import SparkSession

# Iniciar sesión
spark = (SparkSession
        .builder
        .appName("Padron")
        .getOrCreate())

# Imports
from pyspark.sql.functions import trim

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

padron_df.show(5)