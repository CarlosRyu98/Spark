import sys
import pyspark
from pyspark.sql import SparkSession

# Iniciar sesión
spark = (SparkSession
        .builder
        .appName("PySparkSubmit")
        .enableHiveSupport() # Cuidado
        .master(sys.argv[2])
        .config("spark.submit.deployMode", "cluster")
        .getOrCreate())

# Imports
from pyspark.sql.functions import trim

# Importar datos
ruta = sys.argv[1] + "Rango_Edades_Seccion_202012.csv" # Mirar ruta

padron_df_raw = (spark.read.format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .option("delimiter", ";")
                .option("quotes", "\"")
                .option("emptyValue", 0)
                .load(ruta))

# Limpiar datos
padron_df = (padron_df_raw.select([(trim(i[0])).alias(i[0]) if i[1] == "string" else i[0] for i in padron_df_raw.select("*").dtypes]))

# Crear database
spark.sql("CREATE DATABASE IF NOT EXISTS padron")
spark.sql("USE padron")

# Crear view temporal y guardarla como tabla
padron_df.createOrReplaceTempView("padronSpark")

spark.sql("DROP TABLE IF EXISTS padron.padronSpark")
spark.sql("CREATE EXTERNAL TABLE padron.padronSpark STORED AS PARQUET LOCATION '" + sys.argv[1] + "/tablas/'  AS SELECT * FROM padronSpark") #Mirar ruta

# Devolver una query que consuma gran cantidad de recursos
spark.sql("""SELECT a.DESC_DISTRITO, a.DESC_BARRIO, SUM(a.espanoleshombres + a.espanolesmujeres) AS SumaEspanoles, CASE
                  WHEN SUM(a.espanoleshombres) < SUM(a.espanolesmujeres) THEN SUM(a.espanolesmujeres - a.espanoleshombres)
                  WHEN SUM(a.espanoleshombres) > SUM(a.espanolesmujeres) THEN SUM(a.espanoleshombres - a.espanolesmujeres)
                  ELSE 0
              END AS DiferenciaEspanoles
              FROM (SELECT * 
                  FROM padronSpark c
                  WHERE c.espanoleshombres <> 0 AND c.espanolesmujeres <> 0) a
              FULL OUTER JOIN padronSpark b ON (a.COD_DISTRITO == b.COD_DISTRITO)
              WHERE a.espanoleshombres < 10 AND a.espanolesmujeres < 10
              GROUP BY a.DESC_DISTRITO, a.DESC_BARRIO
              HAVING SumaEspanoles % 2 == 0 OR SumaEspanoles % 2 == 1
              ORDER BY DiferenciaEspanoles ASC, SumaEspanoles DESC""").show()

# Meterle caña a tope