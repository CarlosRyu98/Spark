import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object App{

  def main(args : Array[String]) {

    // Iniciar sesión
    spark = (SparkSession
            .builder
            .appName("ScalaSparkHive")
            .getOrCreate)

    // Leer datos
    padron_df = (spark.read.format("parquet")
                .option("header", "true")
                .option("inferSchema", "true")
                .load("hdfs://192.168.10.62:9000/carlos/outScala/PadronParquet"))

    // Crear view temporal y guardarla como tabla
    padron_df.createOrReplaceTempView("padronScalaSpark")

    spark.sql("CREATE TABLE scala.padronSpark as SELECT * FROM padronScalaSpark")

    // Leer los datos de la tabla
    spark.sql("SELECT * FROM scala.padronSpark")

  }

}