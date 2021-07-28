package scripts;

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * @author ${user.name}
 */
object ScalaSparkSubmit {

  def main(args : Array[String]) {

    // Iniciar session
    val spark = SparkSession
    .builder
    .appName("ScalaSparkSubmit")
    .enableHiveSupport()
    .master(args(1))
    .config("spark.submit.deployMode", "cluster")
    .getOrCreate()

    // Definir entrada
    val inputPath = args(0) + "Rango_Edades_Seccion_202012.csv"

    // Crear schema para importar los datos
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

    // Importar los datos
    val padron_df_raw = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "false")
    .option("delimiter", ";")
    .option("quotes", "\"")
    .option("ignoreTrailingWhiteSpace", "true") // No funciona
    .option("ignoreLeadingWhiteSpace", "true") // No funciona
    .option("emptyValue", 0)
    .schema(schema)
    .load(inputPath)


    // Borrar espacios en blanco
    val padron_df = padron_df_raw.select(schema.fields.map(field => {
      if (field.dataType == StringType) {
        trim(col(field.name)).as(field.name)
      } else {
        col(field.name)
      }
    }):_*)

    // Crear database
    spark.sql("CREATE DATABASE IF NOT EXISTS padron")
    spark.sql("USE padron")

    // Crear view temporal y guardarla como tabla
    padron_df.createOrReplaceTempView("padronSpark")

    spark.sql("DROP TABLE IF EXISTS padron.padronSpark")
    spark.sql("CREATE EXTERNAL TABLE padron.padronSpark STORED AS PARQUET LOCATION '" + args(0) + "/tablas/'  AS SELECT * FROM padronSpark")

    // Devolver una query que consuma gran cantidad de recursos
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

  }

}