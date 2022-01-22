import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.GBTClassifier
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler, VectorIndexer}
import org.apache.spark.sql.DataFrame

object Gradient_Boosted_Trees extends Util
{
  def gradient_boosted_trees(df0:DataFrame)=
  {
    val df=df0.na.fill(0.0)

    val featurecols: Array[String] = Array("Duration","no_of_clicks", "unique_clicks", "Max_Time_Spent", "Items_with_2_clicks","Items_with_3_or_more_clicks", "Items_of_Category_zero","Items_of_Category_1_to_12","Items_of_Category_13","Day_Indicator1","Day_Indicator2","Day_Indicator3","Day_Indicator4","Day_Indicator5","Day_Indicator6","Day_Indicator7","Hour_Indicator1","Hour_Indicator2","Hour_Indicator3","Hour_Indicator4","Hour_Indicator5","Hour_Indicator6","Feature_11","Feature_13","Feature_14","Feature_17","Feature_16","Feature_15","Feature_18","Feature_19")
    val assembler=new VectorAssembler().setInputCols(featurecols)
      .setOutputCol("features")

    df.printSchema()

    val df1=assembler.transform(df)
    val labelIndexer = new StringIndexer().setInputCol("buy_indicator").setOutputCol("label")
    val df2=labelIndexer.fit(df1).transform(df1)


    val labelIndexer1=new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(df2)
    // val df3=  labelIndexer1.fit(df2).transform(df2)

    val featureIndexer=new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(2)
      .fit(df2)
    //val df4=featureIndexer.fit(df3).transform(df3)

    val Array(training,test)  =df2.randomSplit(Array(0.9,0.1),100)

    val x = 0.86

    val gbt= new GBTClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")
      .setMaxIter(100)
      .setMaxDepth(10)
      .setMaxBins(10)

    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer1.labels)


    val pipeline=new Pipeline()
      .setStages(Array(labelIndexer1,featureIndexer,gbt,labelConverter))


    val model=pipeline.fit(training)

    val predictions=model.transform(test)

    (predictions)
  }



}
