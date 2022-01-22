import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.DataFrame

object Neural_Network
{
  def neural_net(df0:DataFrame)=
  {
    val df=df0.na.fill(0.0)
    val featurecols: Array[String] = Array("Duration","no_of_clicks", "unique_clicks", "Max_Time_Spent", "Items_with_2_clicks","Items_with_3_or_more_clicks", "Items_of_Category_zero","Items_of_Category_1_to_12","Items_of_Category_13","Day_Indicator1","Day_Indicator2","Day_Indicator3","Day_Indicator4","Day_Indicator5","Day_Indicator6","Day_Indicator7","Hour_Indicator1","Hour_Indicator2","Hour_Indicator3","Hour_Indicator4","Hour_Indicator5","Hour_Indicator6","Feature_11","Feature_13","Feature_14","Feature_17","Feature_16","Feature_15","Feature_18","Feature_19")
    val assembler=new VectorAssembler().setInputCols(featurecols)
      .setOutputCol("features")
    val df1=assembler.transform(df)
    val labelIndexer = new StringIndexer().setInputCol("buy_indicator").setOutputCol("label")
    val df2=labelIndexer.fit(df1).transform(df1)
    df2.show(false)

    val Array(training,test)  =df2.randomSplit(Array(0.9,0.1),100)
    val layers=Array[Int](30,40,2)
    val trainer=new MultilayerPerceptronClassifier()
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setLayers(layers)
      .setBlockSize(128)
      .setSeed(1234L)
      .setMaxIter(200)

    val model=trainer.fit(training)
    val results=model.transform(test)

    (results)
  }


}
