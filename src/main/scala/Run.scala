import Reader._
import Feature_Extraction._
import Feature_Scaling._
import Evaluator._
import Logistic_Regression.logistic_regression
import Random_Forest._
import Gradient_Boosted_Trees._
import Neural_Network._
import Logistic_Regression_Item_Model.logistic_regression_item
import Primary_Reco_System.recommendations

object Run extends Util
{
  def main(args: Array[String]): Unit =
  {
    val ts=System.currentTimeMillis()
//=======================================================Reading Data and Extracting Features==============================//
     val(clicks,buys,durations)=readData()

     val (session_features,item_features)=extracting_features(clicks,buys,durations)
     val combined_item_features=item_features.join(session_features.withColumnRenamed("Duration","Session_Duration"),"Session_ID")

//========================================================Feature Scaling==================================================//
     val scaled_session_features=session_feature_scaling(session_features)
     val scaled_item_features=item_feature_scaling(item_features)

     val scaled_combined_item_features=scaled_item_features.join(scaled_session_features.withColumnRenamed("Duration","Session_Duration"),"Session_ID")

//############################################################################################################################//


//===============================================Starting Model Testing====================================================//

//===============================================1. Logistic Regression Session Model======================================//

    val LR_session_predictions=logistic_regression(scaled_session_features)

    evaluate(LR_session_predictions)

//===============================================2. Random Forest Session Model============================================//
    val RF_session_predictions=session_random_forest(session_features)
    evaluate(RF_session_predictions)

//===============================================3. Gradient Boosted Trees=================================================//
    val GBT_session_predictions=gradient_boosted_trees(session_features)
    evaluate(GBT_session_predictions)

//===============================================4. Neural Network Session Model==========================================//
    val NN_session_prediction=neural_net(scaled_session_features)
    evaluate(NN_session_prediction)
//========================================================================================================================//


//===============================================1. Logistic Regression Item Model========================================//
    val LR_item_prediction=logistic_regression_item(scaled_combined_item_features)
    evaluate(LR_item_prediction)

//===============================================2. Random Forest Item Model===============================================//
    val RF_item_prediction=item_random_forest(combined_item_features)
    evaluate(RF_item_prediction)



//=================================================Recommendations======================================================//

     recommendations(scaled_session_features,scaled_combined_item_features,buys)

//------------------------------------------------------------------------------------------------------------------------//
    val te = System.currentTimeMillis()
    println("Time taken for Code to Run : " + (te - ts) / 1000 + "s")

  }
}
