import org.apache.spark.sql.DataFrame
import Feature_Scaling._
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.functions.udf
object Logistic_Regression
{
  def balanceDataset(dataset: DataFrame): DataFrame =
  {

    // Re-balancing (weighting) of records to be used in the logistic loss objective function
    val numPositives = dataset.filter(dataset("label") === 1).count
    val datasetSize = dataset.count
    val balancingRatio = (datasetSize - numPositives).toDouble / datasetSize

    val calculateWeights = udf { d: Double =>
      if (d == 1.0) {
        0.9 * balancingRatio
      }
      else {
        (0.9 * (1.0 - (balancingRatio.toDouble)))
      }
    }

    val weightedDataset = dataset.withColumn("classWeightCol", calculateWeights(dataset("label")))
    weightedDataset
  }


  def logistic_regression(session_features:DataFrame)=
  {




    val df=session_features.na.fill(0.0)
    val featurecols: Array[String] = Array("Duration","no_of_clicks", "unique_clicks", "Max_Time_Spent", "Items_with_2_clicks","Items_with_3_or_more_clicks", "Items_of_Category_zero","Items_of_Category_1_to_12","Items_of_Category_13","Day_Indicator1","Day_Indicator2","Day_Indicator3","Day_Indicator4","Day_Indicator5","Day_Indicator6","Day_Indicator7","Hour_Indicator1","Hour_Indicator2","Hour_Indicator3","Hour_Indicator4","Hour_Indicator5","Hour_Indicator6","Feature_11","Feature_13","Feature_14","Feature_17","Feature_16","Feature_15","Feature_18","Feature_19")
    val assembler=new VectorAssembler().setInputCols(featurecols)
      .setOutputCol("features")
    val df1=assembler.transform(df)
    val labelIndexer = new StringIndexer().setInputCol("buy_indicator").setOutputCol("label")
    val df2=labelIndexer.fit(df1).transform(df1)

    val df3=balanceDataset(df2)

    val Array(training,test)  =df3.randomSplit(Array(0.9,0.1),100)

    val lr=new LogisticRegression()
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setThreshold(0.55)
      .setMaxIter(100)


    val model=lr.fit(training)

    val results=model.transform(test)

    (results)

  }
}
