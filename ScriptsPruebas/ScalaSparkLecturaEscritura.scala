import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * @author ${user.name}
 */
object App {

  def main(args : Array[String]) {

    // Iniciar session
    val spark = SparkSession
    .builder
    .appName("ScalaLecturaEscritura")
    .getOrCreate()

    // Definir entrada
    val inputPath = "hdfs://192.168.10.62:9000/carlos/datos/Rango_Edades_Seccion_202012.csv"

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

    // Guardar en formato parquet
    padron_df.write.format("parquet").mode("overwrite").save("hdfs://192.168.10.62:9000/carlos/outScala/PadronParquet")

    // Guardar en formato json
    padron_df.write.format("json").mode("overwrite").save("hdfs://192.168.10.62:9000/carlos/outScala/PadronJson")
 
  }

}