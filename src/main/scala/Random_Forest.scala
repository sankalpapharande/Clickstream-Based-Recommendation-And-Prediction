import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.DataFrame

object Random_Forest extends Util
{
  def session_random_forest(df0:DataFrame)=
  {

    val df=df0.na.fill(0.0)
    val featurecols: Array[String] = Array("Duration","no_of_clicks", "unique_clicks", "Max_Time_Spent", "Items_with_2_clicks","Items_with_3_or_more_clicks", "Items_of_Category_zero","Items_of_Category_1_to_12","Items_of_Category_13","Day_Indicator1","Day_Indicator2","Day_Indicator3","Day_Indicator4","Day_Indicator5","Day_Indicator6","Day_Indicator7","Hour_Indicator1","Hour_Indicator2","Hour_Indicator3","Hour_Indicator4","Hour_Indicator5","Hour_Indicator6","Feature_11","Feature_13","Feature_14","Feature_17","Feature_16","Feature_15","Feature_18","Feature_19")
    val assembler=new VectorAssembler().setInputCols(featurecols).setOutputCol("features")
    val df1=assembler.transform(df)
    val labelIndexer = new StringIndexer().setInputCol("buy_indicator").setOutputCol("label")
    val df2=labelIndexer.fit(df1).transform(df1)
    val Array(training,test)  =df2.randomSplit(Array(0.9,0.1),100)

    val x=0.86
    val rf=new RandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setNumTrees(100)
      .setFeatureSubsetStrategy("log2")
      .setImpurity("entropy")
      .setMaxDepth(6)
      .setMaxBins(50)
      .setThresholds(Array(x,1-x))
    val model=rf.fit(training)
    val predictions=model.transform(test)
    (predictions)
  }

  def item_random_forest(df0:DataFrame)=
  {
    val df=df0.na.fill(0.0)
    val featurecols: Array[String] = Array("Feature_1","Feature_7","Feature_8","Feature_9","Feature_10","Feature_11","Last_Indicator","Category_0_Indicator","Category_1to12_Indicator","Category_13_Indicator","Session_Duration","no_of_clicks", "unique_clicks", "Max_Time_Spent", "Items_with_2_clicks","Items_with_3_or_more_clicks", "Items_of_Category_zero","Items_of_Category_1_to_12","Items_of_Category_13","Day_Indicator1","Day_Indicator2","Day_Indicator3","Day_Indicator4","Day_Indicator5","Day_Indicator6","Day_Indicator7","Hour_Indicator1","Hour_Indicator2","Hour_Indicator3","Hour_Indicator4","Hour_Indicator5","Hour_Indicator6","Feature_11","Feature_13","Feature_14","Feature_17","Feature_16","Feature_15","Feature_18","Feature_19")
    val assembler=new VectorAssembler().setInputCols(featurecols).setOutputCol("features")
    val df1=assembler.transform(df)
    val labelIndexer = new StringIndexer().setInputCol("buy_indicator").setOutputCol("label")
    val df2=labelIndexer.fit(df1).transform(df1)
    val Array(training,test)  =df2.randomSplit(Array(0.9,0.1),100)

    val x=0.89

    val rf=new RandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setNumTrees(400)
      .setFeatureSubsetStrategy("0.8")
      .setImpurity("gini")
      .setMaxDepth(6)
      .setMaxBins(90)
      .setThresholds(Array(x,1-x))

    val model=rf.fit(training)
    val predictions=model.transform(test)

    (predictions)
  }
}
