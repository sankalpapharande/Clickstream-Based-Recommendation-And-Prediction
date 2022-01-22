import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.DataFrame
import Feature_Scaling._
object Logistic_Regression_Item_Model extends Util
{
  def logistic_regression_item(training_data0:DataFrame)=
  {
    import spark.implicits._
    val training_data=item_feature_scaling(training_data0)
    val featurecols: Array[String] = Array("Feature_1", "Feature_7", "Feature_8", "Feature_9", "Feature_10", "Feature_11", "Last_Indicator", "Category_0_Indicator", "Category_1to12_Indicator", "Category_13_Indicator", "Session_Duration", "no_of_clicks", "unique_clicks", "Max_Time_Spent", "Items_with_2_clicks", "Items_with_3_or_more_clicks", "Items_of_Category_zero", "Items_of_Category_1_to_12", "Items_of_Category_13", "Day_Indicator1", "Day_Indicator2", "Day_Indicator3", "Day_Indicator4", "Day_Indicator5", "Day_Indicator6", "Day_Indicator7", "Hour_Indicator1", "Hour_Indicator2", "Hour_Indicator3", "Hour_Indicator4", "Hour_Indicator5", "Hour_Indicator6", "Feature_11", "Feature_13", "Feature_14", "Feature_17", "Feature_16", "Feature_15", "Feature_18", "Feature_19")


    val assembler = new VectorAssembler().setInputCols(featurecols)
      .setOutputCol("features")

    val df = assembler.transform(training_data.na.drop)


    val labelIndexer = new StringIndexer().setInputCol("buy_indicator").setOutputCol("label")
    val df2 = labelIndexer.fit(df).transform(df)
    df2.cache()

    val Array(training1, test1) = df2.randomSplit(Array(0.9, 0.1))
    training1.cache()

    val lr = new LogisticRegression()
      .setMaxIter(5)
      .setThreshold(0.85)

    val model: LogisticRegressionModel = lr.fit(training1)

    val predictions = model.transform(test1)

    (predictions)
  }
}
