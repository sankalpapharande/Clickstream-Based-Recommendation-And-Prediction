import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType

object Feature_Extraction extends Util
{
  def extracting_features(clicks:DataFrame, buys:DataFrame,durations:DataFrame) =
  {
    import spark.implicits._
    clicks.createOrReplaceTempView("clicks1")
    durations.createOrReplaceTempView("final_durations")
    buys.createOrReplaceTempView("buys")

    println("-------------------------------------------Started Extracting Session Statistics---------------------------------")

    //===============================Getting Features [Duration, no of clicks, unique clicks,Max time spent]===================//
    val clicks2 = spark.sql("SELECT Session_ID , SUM(Missing_Duration_filled) AS Duration," +
      "COUNT(Item_ID) AS no_of_clicks, COUNT(DISTINCT Item_ID) AS unique_clicks," +
      "MAX(Missing_Duration_filled) AS Max_Time_Spent" +
      " FROM final_durations GROUP BY Session_ID ORDER BY Session_ID")

    clicks2.createOrReplaceTempView("clicks2")

    //=================================================Getting Count of items with 2 clicks or 2 or more clicks===============//
    //Feature 6: No of Items with 2 clicks
    val s: DataFrame = spark.sql("SELECT Session_ID,Item_ID, COUNT(1),IF(COUNT(1)=2,COUNT(1),0) AS CNT FROM clicks1 GROUP BY Session_ID, Item_ID ORDER BY Session_ID")
    s.createOrReplaceTempView("s")

    val t = spark.sql("SELECT Session_ID,IF(CNT=2, COUNT(CNT),0) AS CNT2 FROM s GROUP BY Session_ID,CNT ORDER BY Session_ID")
    t.createOrReplaceTempView("t")

    val two_clicks = spark.sql("SELECT Session_ID, MAX(CNT2) AS Items_with_2_clicks FROM t GROUP BY Session_ID ORDER BY Session_ID")


    //Feature 7: No of Items with 3 clicks
    val a: DataFrame = spark.sql("SELECT Session_ID,Item_ID, COUNT(1),IF(COUNT(1)>=3,COUNT(1),0) AS CNT FROM clicks1 GROUP BY Session_ID, Item_ID ORDER BY Session_ID")
    a.createOrReplaceTempView("a")
    val b = spark.sql("SELECT Session_ID,IF(CNT=3, COUNT(CNT),0) AS CNT3 FROM a GROUP BY Session_ID,CNT ORDER BY Session_ID")
    b.createOrReplaceTempView("b")
    val three_or_more_clicks = spark.sql("SELECT Session_ID, MAX(CNT3) AS Items_with_3_or_more_clicks FROM b GROUP BY Session_ID ORDER BY Session_ID")


    val no_of_clicks = two_clicks.join(three_or_more_clicks, "Session_ID")

    //=====================================Getting Category Wise Count==========================================================//
    //Feature 8: No of Items with Category 0
    val c = spark.sql("SELECT Session_ID, IF(Category = 0 AND COUNT(DISTINCT Item_ID) != 0 ,COUNT(DISTINCT Item_ID),0) AS category_zero FROM clicks1 GROUP BY Session_ID,Category ORDER BY Session_ID  ")
    c.createOrReplaceTempView("c")
    val cat_zero = spark.sql("SELECT Session_ID , MAX(category_zero) AS Items_of_Category_zero FROM c GROUP BY Session_ID ORDER BY Session_ID")


    //Feature 9: No of Items with Category 13
    val d = spark.sql("SELECT Session_ID, IF(Category = 13 AND COUNT(DISTINCT Item_ID) != 0 ,COUNT(DISTINCT Item_ID),0) AS category_zero FROM clicks1 GROUP BY Session_ID,Category ORDER BY Session_ID  ")
    d.createOrReplaceTempView("d")
    val cat_13 = spark.sql("SELECT Session_ID , MAX(category_zero) AS Items_of_Category_13 FROM d GROUP BY Session_ID ORDER BY Session_ID")


    //Feature 10: No of Items with Category Between 1 and 12
    val e = spark.sql("SELECT Session_ID, IF(Category BETWEEN 1 AND 12 AND COUNT(DISTINCT Item_ID) != 0 ,COUNT(DISTINCT Item_ID),0) AS category_zero FROM clicks1 GROUP BY Session_ID,Category ORDER BY Session_ID ")
    e.createOrReplaceTempView("e")
    val cat_0_to_12 = spark.sql("SELECT Session_ID , MAX(category_zero) AS Items_of_Category_1_to_12 FROM e GROUP BY Session_ID ORDER BY Session_ID")

    val category_count = cat_zero.join(cat_0_to_12, "Session_ID").join(cat_13, "Session_ID")


    //================================================Getting Time Features=================================================//
    val clicks3 = spark.sql("SELECT Session_ID, MIN(Standard_Form) AS Standard_Form FROM clicks1 GROUP BY Session_ID ORDER BY Session_ID")
      .withColumn("Time_in_seconds", unix_timestamp($"Standard_Form"))
      .withColumn("split", split(col("Standard_Form"), " "))
      .withColumn("Date", col("split")(0))
      .withColumn("Time", col("split")(1))
      .withColumn("Day_of_the_week", from_unixtime(unix_timestamp($"Date", "yyyy-MM-dd"), "EEEEE"))
      .withColumn("Day_Indicator1", when($"Day_of_the_week" === "Sunday", 1).otherwise(0))
      .withColumn("Day_Indicator2", when($"Day_of_the_week" === "Monday", 1).otherwise(0))
      .withColumn("Day_Indicator3", when($"Day_of_the_week" === "Tuesday", 1).otherwise(0))
      .withColumn("Day_Indicator4", when($"Day_of_the_week" === "Wednesday", 1).otherwise(0))
      .withColumn("Day_Indicator5", when($"Day_of_the_week" === "Thursday", 1).otherwise(0))
      .withColumn("Day_Indicator6", when($"Day_of_the_week" === "Friday", 1).otherwise(0))
      .withColumn("Day_Indicator7", when($"Day_of_the_week" === "Saturday", 1).otherwise(0))
      .withColumn("Hour", hour(col("Time")))
      .withColumn("Hour_Indicator1", when($"Hour" <= 4 && $"Hour" >= 0, 1).otherwise(0))
      .withColumn("Hour_Indicator2", when($"Hour" <= 8 && $"Hour" >= 5, 1).otherwise(0))
      .withColumn("Hour_Indicator3", when($"Hour" <= 12 && $"Hour" >= 9, 1).otherwise(0))
      .withColumn("Hour_Indicator4", when($"Hour" <= 16 && $"Hour" >= 13, 1).otherwise(0))
      .withColumn("Hour_Indicator5", when($"Hour" <= 20 && $"Hour" >= 17, 1).otherwise(0))
      .withColumn("Hour_Indicator6", when($"Hour" <= 24 && $"Hour" >= 21, 1).otherwise(0))
      .drop("Standard_Form", "split", "Hour", "Time_in_seconds", "Date", "Time", "Day_of_the_week")


    var clicks4 = clicks2.join(no_of_clicks, "Session_ID").join(category_count, "Session_ID").join(clicks3, "Session_ID")

    clicks4.createOrReplaceTempView("clicks")


    //=======================================================================================================================//
    //===============================================GLOBAL SESSION FEATURES AND ITEM FEATURES===============================//
    //========================================================================================================================//
    println("-----------------------------------Started Global & Item Features---------------------------------------")

    val session_count_clicks: DataFrame =spark.sql("SELECT DISTINCT(Item_ID), " +
      "COUNT(DISTINCT Session_ID) AS `click(.,dnm)` " +
      "FROM clicks GROUP BY Item_ID ORDER BY Item_ID")
    val session_count_buys=spark.sql("SELECT DISTINCT(Item_ID)," +
      "COUNT(DISTINCT Session_ID) AS `buy(.,dnm)` " +
      "FROM buys GROUP BY Item_ID ORDER BY Item_ID")


    var item_features: Dataset[Row] =spark.sql("SELECT Session_ID, Item_ID,Category, COUNT(Item_ID) AS `click(sn,dnm)` FROM clicks GROUP BY Session_ID, Item_ID,Category").sort("Session_ID")


    item_features=item_features.join(session_count_clicks,"Item_ID")
      .withColumnRenamed("Item_ID","Item_ID1")
      .join(session_count_buys,$"Item_ID1"===$"Item_ID","left_outer")
      .drop("Item_ID")
      .withColumnRenamed("Item_ID1","Item_ID")
    item_features=item_features
      .withColumn("buy(.,dnm)*click(sn,dnm)",$"`buy(.,dnm)`".multiply($"`click(sn,dnm)`"))
      .withColumn("buy(.,dnm)/(click(.,dnm)+beta)",$"`buy(.,dnm)`".divide($"`click(.,dnm)`"+10))
      .withColumn("buy(.,dnm)*click(sn,dnm)/(click(.,dnm)+beta)",$"`buy(.,dnm)/(click(.,dnm)+beta)`".multiply($"`click(sn,dnm)`"))

    item_features.createOrReplaceTempView("item_features")


    //====================================================Last Indicator & Duration added======================================//

    val last=spark.sql("SELECT Session_ID,MAX(Standard_Form) AS Last FROM clicks GROUP BY Session_ID ORDER BY Session_ID")
    val last1=clicks.drop("Date","Time","Category","Day_of_the_week","Hour").join(last,"Session_ID")
    last1.createOrReplaceTempView("last1")
    val last_indicator=spark.sql("SELECT Session_ID, Item_ID, IF(Standard_Form==Last,1,0) AS Last_Indicator " +
      "FROM last1 GROUP BY Session_ID,Item_ID,Standard_Form,Last ORDER BY Session_ID")


    item_features=item_features.join(last_indicator,Seq("Session_ID","Item_ID")).join(durations,Seq("Session_ID","Item_ID"))


    //=======================================================Global Session Features=============================================//

    val filtered=item_features.select("Session_ID","`click(.,dnm)`","`buy(.,dnm)*click(sn,dnm)/(click(.,dnm)+beta)`")
      .filter($"Last_Indicator"===1)
      .distinct()
      .withColumnRenamed("click(.,dnm)","click(.,dnM)")
      .withColumnRenamed("buy(.,dnm)*click(sn,dnm)/(click(.,dnm)+beta)","buy(.,dnM)*click(sn,dnM)/(click(.,dnM)+beta)")

    var global_features=spark.sql("SELECT Session_ID,SUM(`click(.,dnm)`) AS `Sigma(click(.,dnm))`, SUM(`buy(.,dnm)`) AS `Sigma(buy(.,dnm))`," +
      " SUM(`buy(.,dnm)*click(sn,dnm)`) AS `Sigma(buy(.,dnm)*click(sn,dnm))` ," +
      "SUM(`buy(.,dnm)*click(sn,dnm)/(click(.,dnm)+beta)`) AS `Sigma(buy(.,dnm)*click(sn,dnm)/(click(.,dnm)+beta))`, " +
      "SUM(`buy(.,dnm)/(click(.,dnm)+beta)`) AS `Sigma(buy(.,dnm)/(click(.,dnm)+beta))`, "+
      // "`click(.,dnm)` AS `click(.,dnM)` WHERE Last_Indicator==1 ," +
      "MAX(`buy(.,dnm)/(click(.,dnm)+beta)`) AS `MAX(buy(.,dnm)/(click(.,dnm)+beta))` " +
      "FROM item_features GROUP BY Session_ID ORDER BY Session_ID")
    global_features=global_features.join(filtered,"Session_ID")

    //================================================Changing Column names===================================================//
    global_features=global_features.withColumnRenamed("Sigma(click(.,dnm))","Feature_11")
      .withColumnRenamed("Sigma(buy(.,dnm))","Feature_13")
      .withColumnRenamed("Sigma(buy(.,dnm)*click(sn,dnm))","Feature_14")
      .withColumnRenamed("Sigma(buy(.,dnm)*click(sn,dnm)/(click(.,dnm)+beta))","Feature_17")
      .withColumnRenamed("Sigma(buy(.,dnm)/(click(.,dnm)+beta))","Feature_16")
      .withColumnRenamed("MAX(buy(.,dnm)/(click(.,dnm)+beta))","Feature_15")
      .withColumnRenamed("click(.,dnM)","Feature_18")
      .withColumnRenamed("buy(.,dnM)*click(sn,dnM)/(click(.,dnM)+beta)","Feature_19")

    //================================================Item Features=========================================================//
    item_features=item_features.withColumnRenamed("click(sn,dnm)","Feature_1")
      .withColumnRenamed("click(.,dnm)","Feature_7")
      .withColumnRenamed("buy(.,dnm)","Feature_8")
      .withColumnRenamed("buy(.,dnm)*click(sn,dnm)","Feature_9")
      .withColumnRenamed("buy(.,dnm)/(click(.,dnm)+beta)","Feature_10")
      .withColumnRenamed("buy(.,dnm)*click(sn,dnm)/(click(.,dnm)+beta)","Feature_11")
      .withColumn("Category_0_Indicator",when($"Category"===0,1).otherwise(0))
      .withColumn("Category_1to12_Indicator",when($"Category"<=12 && $"Category">=1,1).otherwise(0))
      .withColumn("Category_13_Indicator",when($"Category"===13,1).otherwise(0))
      .drop("Category")

    var session_features=clicks4.join(global_features,"Session_ID")


    val bought_sessions=spark.sql("SELECT DISTINCT(Session_ID) FROM buys GROUP BY Session_ID ORDER BY Session_ID")
    bought_sessions.createOrReplaceTempView("bought_sessions")

    session_features=session_features.withColumnRenamed("Session_ID","Session_ID1")
      .join(bought_sessions,$"Session_ID1"===$"Session_ID","left_outer")
      .withColumnRenamed("Session_ID","common")
      .withColumnRenamed("Session_ID1","Session_ID")
      .withColumn("buy_indicator",when(col("common").isNull,0).otherwise(1))
    session_features.createOrReplaceTempView("session_features1")


//============================Casting all columns to DoubleType==========================================================//
    session_features=session_features.select($"Session_ID".cast(DoubleType),$"Duration".cast(DoubleType).cast(DoubleType),$"no_of_clicks".cast(DoubleType),$"unique_clicks".cast(DoubleType), $"Max_Time_Spent".cast(DoubleType), $"Items_with_2_clicks".cast(DoubleType),$"Items_with_3_or_more_clicks".cast(DoubleType), $"Items_of_Category_zero".cast(DoubleType),$"Items_of_Category_1_to_12".cast(DoubleType),$"Items_of_Category_13".cast(DoubleType),$"Day_Indicator1".cast(DoubleType),$"Day_Indicator2".cast(DoubleType),$"Day_Indicator3".cast(DoubleType),$"Day_Indicator4".cast(DoubleType),$"Day_Indicator5".cast(DoubleType),$"Day_Indicator6".cast(DoubleType),$"Day_Indicator7".cast(DoubleType),$"Hour_Indicator1".cast(DoubleType),$"Hour_Indicator2".cast(DoubleType),$"Hour_Indicator3".cast(DoubleType),$"Hour_Indicator4".cast(DoubleType),$"Hour_Indicator5".cast(DoubleType),$"Hour_Indicator6".cast(DoubleType),$"Feature_11".cast(DoubleType),$"Feature_13".cast(DoubleType),$"Feature_14".cast(DoubleType),$"Feature_17".cast(DoubleType),$"Feature_16".cast(DoubleType),$"Feature_15".cast(DoubleType),$"Feature_18".cast(DoubleType),$"Feature_19".cast(DoubleType),$"buy_indicator".cast(DoubleType))
    val item_features_1: DataFrame =item_features.columns.foldLeft(item_features)((current, c) => current.withColumn(c, col(c).cast(DoubleType)))


    (session_features,item_features_1)

  }
}
