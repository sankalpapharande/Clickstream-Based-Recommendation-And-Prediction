import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType

object Reader extends Util
{
  def readData() =
  {
    import spark.implicits._

    val ts = System.currentTimeMillis()
    println("-----------------------------------------------------------------DATA READ BEGIN")

    // Loading Dataset
    val clicks_data: DataFrame = spark.read.csv("modified_clicks.csv")
    val buys_data = spark.read.csv("buys.csv")


    //Changing their column names
    val newname_clicks = Seq("Session_ID", "Timestamp", "Item_ID", "Category")
    val newname_buys = Seq("Session_ID", "Timestamp", "Item_ID", "Price", "Quantity")
    val clicks: DataFrame = clicks_data.toDF(newname_clicks: _*)
    val buys: DataFrame = buys_data.toDF(newname_buys: _*)
    val te = System.currentTimeMillis()


    //Deploying as clicks and buys sets
    clicks.createOrReplaceTempView("clicks")
    buys.createOrReplaceTempView("buys")

//=========================================Postprocessing================================================================//
    var clicks1= clicks.select($"Session_ID",$"Timestamp",unix_timestamp($"Timestamp","yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
      .cast(TimestampType).as("Standard_Form"),$"Item_ID",$"Category")
      .withColumn("Time_in_seconds",unix_timestamp($"Standard_Form"))
      .withColumn("split",split(col("Standard_Form")," "))
      .withColumn("Date",col("split")(0))
      .withColumn("Time",col("split")(1))
      .withColumn("Day_of_the_week",from_unixtime(unix_timestamp($"Date", "yyyy-MM-dd"),"EEEEE"))
      .withColumn("Hour",hour(col("Time")))

  //Getting durations spent on each item;Average if the item is clicked last
    val z: WindowSpec =Window.partitionBy("Session_ID").orderBy("Time_in_seconds")
    val diff: Column =lag($"Time_in_seconds",-1).over(z)-$"Time_in_seconds"
    clicks1 = clicks1.withColumn("Duration",diff)
    clicks1.createOrReplaceTempView("clicks1")

    val average: DataFrame =spark.sql("SELECT Item_ID, AVG(Duration) AS Average FROM clicks1 GROUP BY Item_ID ORDER BY Item_ID")
    average.createOrReplaceTempView("average")
    val finale: DataFrame =spark.sql("SELECT a.* ,b.avr_dur FROM clicks1 AS a INNER JOIN (SELECT Item_ID, Average AS avr_dur FROM average GROUP BY Item_ID,Average) AS b ON a.Item_ID=b.Item_ID" )
    finale.createOrReplaceTempView("finale")
    var dur=spark.sql("SELECT Session_ID,Item_ID, Duration,IF(Duration IS NULL ,avr_dur, Duration) AS Missing_Duration_filled FROM finale")
    dur.createOrReplaceTempView("final_durations")

    clicks1=clicks1.drop("Duration")

    val buys1: DataFrame =buys.select($"Session_ID",$"Timestamp",unix_timestamp($"Timestamp","yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
      .cast(TimestampType).as("Standard_Form"),$"Item_ID",$"Price",$"Quantity")
      .withColumn("split",split(col("Standard_Form")," "))
      .select(col("Session_ID"),col("Standard_Form"),col("split")(0).as("Date"),col("split")(1).as("Time"),col("Item_ID"),col("Price"),col("Quantity"))
      .withColumn("Day_of_the_week",from_unixtime(unix_timestamp($"Date", "yyyy-MM-dd"),"EEEEE"))
      .withColumn("Hour",hour(col("Time")))



    println("Time taken for DATA READ: " + (te - ts) / 1000 + "s")

    (clicks1,buys1,dur)

  }
}
