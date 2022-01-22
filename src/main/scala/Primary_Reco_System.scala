import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{mean, not, row_number, stddev}
import org.apache.spark.ml.feature.{BucketedRandomProjectionLSH, StringIndexer, VectorAssembler}
import org.apache.spark.sql.expressions.Window

object Primary_Reco_System extends Util
{
  def recommendations(scaled_session_features0:DataFrame,scaled_item_features:DataFrame,buy:DataFrame)=
  {
    import spark.implicits._

    val (mean_1, std_1) = scaled_session_features0.select(mean("no_of_clicks"), stddev("no_of_clicks"))
      .as[(Double, Double)]
      .first()
    val (mean_2, std_2) = scaled_session_features0.select(mean("unique_clicks"), stddev("unique_clicks"))
      .as[(Double, Double)]
      .first()


    val scaled_session_features=scaled_session_features0
      .withColumn("no_of_clicks_normalized",(scaled_session_features0("no_of_clicks")-mean_1)/std_1)
      .withColumn("unique_clicks_normalized",(scaled_session_features0("unique_clicks")-mean_2)/std_2)




    val df=scaled_session_features.na.fill(0.0)
    val featurecols: Array[String] = Array("Duration","no_of_clicks_normalized","unique_clicks_normalized","unique_clicks", "Max_Time_Spent", "Items_with_2_clicks","Items_with_3_or_more_clicks", "Items_of_Category_zero","Items_of_Category_1_to_12","Items_of_Category_13","Day_Indicator1","Day_Indicator2","Day_Indicator3","Day_Indicator4","Day_Indicator5","Day_Indicator6","Day_Indicator7","Hour_Indicator1","Hour_Indicator2","Hour_Indicator3","Hour_Indicator4","Hour_Indicator5","Hour_Indicator6","Feature_11","Feature_13","Feature_14","Feature_17","Feature_16","Feature_15","Feature_18","Feature_19")
    val assembler=new VectorAssembler().setInputCols(featurecols)
      .setOutputCol("features")
    val df1=assembler.transform(df)
    val labelIndexer = new StringIndexer().setInputCol("buy_indicator").setOutputCol("label")
    val df2=labelIndexer.fit(df1).transform(df1)



//==================================================Bucket Random Projections=============================================================//
//==================================================Determining Neighbors=======================================================//

    val threshold = 1.0  //Eucledian Similarity Threshold
    val min_neighbor=7   //Neighbors are determined for those sessions which have min 7 neighbors

    val BRP=new BucketedRandomProjectionLSH()
      .setBucketLength(0.8)
      .setNumHashTables(10)
      .setInputCol("features")
      .setOutputCol("Values")
   val model=BRP.fit(df2)

    val results =  model.approxSimilarityJoin(df2,df2,threshold).select(
      $"datasetA".getField("Session_ID").as("Session_ID1"),
      $"datasetB".getField("Session_ID").as("Session_ID2"),
      $"distCol")

    val temp=results.filter(!($"Session_ID1"===$"Session_ID2"))
    temp.createOrReplaceTempView("temp")
    val temp2=spark.sql("SELECT Session_ID1,COUNT(Session_ID2) AS neighbour_count FROM temp GROUP BY Session_ID1 ORDER BY Session_ID1")

    val neighbors=temp.join(temp2,"Session_ID1")
      .filter($"neighbour_count">=min_neighbor)
      .drop("distCol","neighbour_count")

//================================================Creating Click Buys Combined Dataset=====================================================//

    val clicks=scaled_item_features
      .select("Session_ID","Item_ID","Feature_1")
      .withColumnRenamed("Feature_1","click_count")


    buy.createOrReplaceTempView("buys")

    val buys2=spark.sql("SELECT Session_ID,Item_ID,SUM(Quanti) AS buy_count FROM buys GROUP BY Session_ID,Item_ID")

    val click_buy=clicks.join(buys2,Seq("Session_ID","Item_ID"),"leftouter").na.fill(0.0)
    // click_buy dataframe contains colms [Session_ID, Item_ID, click_count, buy_count]
//========================================================================================================================//

    val click_buy1=click_buy
      .withColumn("value",$"click_count"/20+$"buy_count"*19/20)
      .drop("click_count","buy_count")


    val required_sessions=neighbors.select("Session_ID2").distinct()

    val data: DataFrame = click_buy1.join(required_sessions,$"Session_ID"===$"Session_ID2")

    val req_data=data.drop("Session_ID2")

    val final_dataframe=neighbors.join(req_data,$"Session_ID2"===$"Session_ID")

    final_dataframe.createOrReplaceTempView("df3")

    var reco=spark.sql("SELECT Session_ID1,Item_ID,SUM(value) AS net_value FROM df3 GROUP BY Session_ID1,Item_ID ORDER BY Session_ID1")
    reco.createOrReplaceTempView("temp")


//=========================================================Max n recommendations=======================================================//
    val n = 10
    val w = Window.partitionBy($"Session_ID1").orderBy($"net_value".desc)

    val reco_items: DataFrame =reco.withColumn("rn",row_number().over(w)).where($"rn"<=n).drop("rn")


//==================================================Reco Evaluator========================================================//
    val check=reco_items.withColumnRenamed("Session_ID1","Session_ID")
      .join(click_buy,Seq("Session_ID","Item_ID"),"leftouter")
    check.createOrReplaceTempView("check")

    val hit_sessions: Long = check.filter($"click_count".isNotNull).select("Session_ID").distinct().count()

//============================================================= Session Level Hit Rate=======================================================//

    println("% Hit_Rate @"+n+" = "+hit_sessions.toDouble*100/9249729)



  }
}
