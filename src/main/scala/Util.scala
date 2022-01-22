import org.apache.spark.sql.SparkSession
trait Util
{
  final protected val spark=SparkSession
    .builder()
    .appName("Spark Reco")
    .config("spark.master","local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
}
