import sys
import pyspark
from pyspark.sql import SparkSession

# Iniciar sesión
spark = (SparkSession
        .builder
        .appName("PySparkHive")
        .enableHiveSupport()
        .getOrCreate())

# Leer datos
padron_df = (spark.read.format("parquet")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(sys.argv[1] + "/outPy/PadronParquet"))

# Crear view temporal y guardarla como tabla
padron_df.createOrReplaceTempView("padronSpark")

spark.sql("CREATE TABLE padron.padronSpark as SELECT * FROM padronSpark")

# Leer los datos de la tabla
spark.sql("SELECT * FROM padron.padronSpark").show()