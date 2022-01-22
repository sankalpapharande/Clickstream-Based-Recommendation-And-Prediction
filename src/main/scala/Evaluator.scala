import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.not

object Evaluator extends Util
{
  def evaluate(predictions:DataFrame)
  {
    import spark.implicits._
    predictions.cache()
    //  val evaluator = new BinaryClassificationEvaluator()
    //    .setLabelCol("label")
    //    .setRawPredictionCol("rawPrediction")
    //    .setMetricName("areaUnderROC")
    //  val accuracy: Double = evaluator.evaluate(predictions)
    //  println("Accuracy is " + accuracy)


    val lp = predictions.select("label", "prediction")
    val counttotal: Long = predictions.count()
    val correct: Long = lp.filter($"label" === $"prediction").count()
    val wrong: Long = lp.filter(not($"label" === $"prediction")).count()
    val trueP: Long = lp.filter($"prediction" === 1.0).filter($"label" === $"prediction").count()
    val trueN: Long = lp.filter($"prediction" === 0.0).filter($"label" === $"prediction").count()
    val falseN: Long =lp.filter($"prediction" === 0.0).filter(not($"label" === $"prediction")).count()
    val falseP: Long = lp.filter($"prediction" === 1.0).filter(not($"label" === $"prediction")).count()
    println("Total Count: " + counttotal + "\n" +
      "Correctly Classified:" + correct + "\n" +
      "Wrong classified     " + wrong + "\n" +
      "True Positive:       " + trueP + "\n" +
      "True Negative:       " + trueN + "\n" +
      "False Positive:      " + falseP + "\n" +
      "False Negative:      " + falseN)
    val precision=trueP.toDouble/(trueP+falseP)
    val recall=trueP.toDouble/(trueP+falseN)
    val F1 = 2 * trueP.toDouble / (2 * trueP + falseP + falseN)
    print("\n")
    println("-------------------------------------------------------------------------------------------")
    println("Precision: "+precision)
    println("Recall:    "+recall)
    println("F1 score:  " + F1)
  }
}
