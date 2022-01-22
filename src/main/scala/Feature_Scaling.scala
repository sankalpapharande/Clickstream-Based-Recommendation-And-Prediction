import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{mean, stddev}

object Feature_Scaling extends Util
{
  def session_feature_scaling(df:DataFrame)=
  {
    import spark.implicits._

    var col_array: Array[String] = df.columns.drop(1).dropRight(1)

    def remove(a: Array[String], i: Int): Array[String] =
    {
      val b = a.toBuffer
      b.remove(i)
      b.toArray
    }


    var newarray = remove(col_array, col_array.indexOf("Day_Indicator1"))
    newarray = remove(newarray, newarray.indexOf("Day_Indicator2"))
    newarray = remove(newarray, newarray.indexOf("Day_Indicator3"))
    newarray = remove(newarray, newarray.indexOf("Day_Indicator4"))
    newarray = remove(newarray, newarray.indexOf("Day_Indicator5"))
    newarray = remove(newarray, newarray.indexOf("Day_Indicator6"))
    newarray = remove(newarray, newarray.indexOf("Day_Indicator7"))

    newarray = remove(newarray, newarray.indexOf("Hour_Indicator1"))
    newarray = remove(newarray, newarray.indexOf("Hour_Indicator2"))
    newarray = remove(newarray, newarray.indexOf("Hour_Indicator3"))
    newarray = remove(newarray, newarray.indexOf("Hour_Indicator4"))
    newarray = remove(newarray, newarray.indexOf("Hour_Indicator5"))
    newarray = remove(newarray, newarray.indexOf("Hour_Indicator6"))
    newarray = remove(newarray, newarray.indexOf("no_of_clicks"))
    newarray = remove(newarray, newarray.indexOf("unique_clicks"))
    newarray = remove(newarray, newarray.indexOf("Items_with_2_clicks"))
    newarray = remove(newarray, newarray.indexOf("Items_with_3_or_more_clicks"))
    newarray = remove(newarray, newarray.indexOf("Items_of_Category_zero"))
    newarray = remove(newarray, newarray.indexOf("Items_of_Category_1_to_12"))
    newarray = remove(newarray, newarray.indexOf("Items_of_Category_13"))

    newarray.foreach(println)

    var finalData: DataFrame = df.cache()
    for (element <- newarray)
    {
      val (mean_, std_) = df.select(mean(element), stddev(element))
        .as[(Double, Double)]
        .first()
      finalData = finalData.withColumn(element, (finalData(element) - mean_) / std_)
    }


    (finalData)

  }

  def item_feature_scaling(df:DataFrame)=
  {
    import spark.implicits._

    var col_array: Array[String] = df.columns.drop(2).dropRight(1)

    def remove(a: Array[String], i: Int): Array[String] =
    {
      val b = a.toBuffer
      b.remove(i)
      b.toArray
    }


    var newarray = remove(col_array, col_array.indexOf("Day_Indicator1"))
    newarray = remove(newarray, newarray.indexOf("Day_Indicator2"))
    newarray = remove(newarray, newarray.indexOf("Day_Indicator3"))
    newarray = remove(newarray, newarray.indexOf("Day_Indicator4"))
    newarray = remove(newarray, newarray.indexOf("Day_Indicator5"))
    newarray = remove(newarray, newarray.indexOf("Day_Indicator6"))
    newarray = remove(newarray, newarray.indexOf("Day_Indicator7"))

    newarray = remove(newarray, newarray.indexOf("Hour_Indicator1"))
    newarray = remove(newarray, newarray.indexOf("Hour_Indicator2"))
    newarray = remove(newarray, newarray.indexOf("Hour_Indicator3"))
    newarray = remove(newarray, newarray.indexOf("Hour_Indicator4"))
    newarray = remove(newarray, newarray.indexOf("Hour_Indicator5"))
    newarray = remove(newarray, newarray.indexOf("Hour_Indicator6"))
    newarray = remove(newarray, newarray.indexOf("no_of_clicks"))
    newarray = remove(newarray, newarray.indexOf("unique_clicks"))
    newarray = remove(newarray, newarray.indexOf("Items_with_2_clicks"))
    newarray = remove(newarray, newarray.indexOf("Items_with_3_or_more_clicks"))
    newarray = remove(newarray, newarray.indexOf("Items_of_Category_zero"))
    newarray = remove(newarray, newarray.indexOf("Items_of_Category_1_to_12"))
    newarray = remove(newarray, newarray.indexOf("Items_of_Category_13"))

    newarray.foreach(println)

    var finalData: DataFrame = df.cache()
    for (element <- newarray)
    {
      val (mean_, std_) = df.select(mean(element), stddev(element))
        .as[(Double, Double)]
        .first()
      finalData = finalData.withColumn(element, (finalData(element) - mean_) / std_)
    }


    (finalData)

  }



}
