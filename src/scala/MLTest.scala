package scala

import breeze.features.FeatureVector
import org.apache.spark.sql.types.StructType
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification._
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorIndexer, IndexToString}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.regression.{RandomForestRegressor, LinearRegression}
import org.apache.spark.sql.{ Row, SparkSession}

/**
  * Created by pi on 16-8-22.
  */
class MLTest {

}

object MLTest{

  def main(args: Array[String]) {

    val ss = SparkSession.builder().appName("Machine Learning Test")
      .master("local[*]").getOrCreate()

    println("----------Logistic Regression Example------------")
    val training = ss.createDataFrame(Seq(
      (1.0, Vectors.dense(0.0, 1.1, 0.1)),
      (0.0, Vectors.dense(2.0, 1.0, -1.0)),
      (0.0, Vectors.dense(2.0, 1.3, 1.0)),
      (1.0, Vectors.dense(0.0, 1.2, -0.5))
    )).toDF("label", "features")
    val lr = new LogisticRegression()
    println("LogisticRegression parameters:\n" + lr.explainParams() + "\n")
    lr.setMaxIter(10)
      .setRegParam(0.01)
    val logicalModel = lr.fit(training)
    println("logicalModel was fit using parameters: " + logicalModel.parent.extractParamMap)

    // We may alternatively specify parameters using a ParamMap,
    // which supports several methods for specifying parameters.
    val paramMap = ParamMap(lr.maxIter -> 20)
      .put(lr.maxIter, 30)  // Specify 1 Param. This overwrites the original maxIter.
      .put(lr.regParam -> 0.1, lr.threshold -> 0.55)  // Specify multiple Params.

    // One can also combine ParamMaps.
    val paramMap2 = ParamMap(lr.probabilityCol -> "myProbability")  // Change output column name.
    val paramMapCombined = paramMap ++ paramMap2

    // Now learn a new model using the paramMapCombined parameters.
    // paramMapCombined overrides all parameters set earlier via lr.set* methods.
    val model2 = lr.fit(training, paramMapCombined)
    println("Model 2 was fit using parameters: " + model2.parent.extractParamMap)

    // Prepare test data.
    val test = ss.createDataFrame(Seq(
      (1.0, Vectors.dense(-1.0, 1.5, 1.3)),
      (0.0, Vectors.dense(3.0, 2.0, -0.1)),
      (1.0, Vectors.dense(0.0, 2.2, -1.5))
    )).toDF("label", "features")

    // Make predictions on test data using the Transformer.transform() method.
    // LogisticRegression.transform will only use the 'features' column.
    // Note that model2.transform() outputs a 'myProbability' column instead of the usual
    // 'probability' column since we renamed the lr.probabilityCol parameter previously.
    model2.transform(test)
      .select("features", "label", "myProbability", "prediction")
      .collect()
      .foreach { case Row(features: org.apache.spark.ml.linalg.Vector ,
      label: Double, prob: org.apache.spark.ml.linalg.Vector , prediction: Double) =>
        println(s"($features, $label) -> prob=$prob, prediction=$prediction")
      }

    println("----------Linear Regression------------")

    val linearTraining = ss.read.format("libsvm")
      .load("/home/pi/doc/Spark/spark-2.0.0-bin-hadoop2.7/data/mllib/test_linear_regression_data.txt")

    //ordinary least squares or linear least squares uses no regularization
    val linearRegression = new LinearRegression()
      .setMaxIter(100)
    //正则化参数
      //.setRegParam(0.5)
    /**
      * Param for the ElasticNet mixing parameter, in range [0, 1].
      * For alpha = 0, the penalty is an L2 penalty. For alpha = 1, it is an L1 penalty.
      * ridge regression uses L2 regularization; and Lasso uses L1 regularization.
      */
      //.setElasticNetParam(0)

    // Fit the model
    val lrModel = linearRegression.fit(linearTraining)

    // Print the coefficients and intercept for linear regression
    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

    // Summarize the model over the training set and print out some metrics
    val trainingSummary = lrModel.summary
    println(s"numIterations: ${trainingSummary.totalIterations}")
    println(s"objectiveHistory: ${trainingSummary.objectiveHistory.toList}")
    trainingSummary.residuals.show()
    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    println(s"r2: ${trainingSummary.r2}")

    println("----------Logistic Regression------------")
    // Load training data
    val logisticTraining = ss.read.format("libsvm").load("/home/pi/doc/Spark/spark-2.0.0-bin-hadoop2.7/data/mllib/sample_libsvm_data.txt")

    val logistic = new LogisticRegression()
      .setMaxIter(10)
      //.setRegParam(0.3)
      //.setElasticNetParam(0.8)

    // Fit the model
    val logisticModel = logistic.fit(logisticTraining)

    // Print the coefficients and intercept for logistic regression
    println(s"Coefficients: ${logisticModel.coefficients} Intercept: ${logisticModel.intercept}")

    // Extract the summary from the returned LogisticRegressionModel instance trained in the earlier
    // example
    val logisticTrainingSummary = logisticModel.summary

    // Obtain the objective per iteration.
    val objectiveHistory = logisticTrainingSummary.objectiveHistory
    objectiveHistory.foreach(loss => println(loss))

    // Obtain the metrics useful to judge performance on test data.
    // We cast the summary to a BinaryLogisticRegressionSummary since the problem is a
    // binary classification problem.
    val binarySummary = logisticTrainingSummary.asInstanceOf[BinaryLogisticRegressionSummary]

    // Obtain the receiver-operating characteristic as a dataframe and areaUnderROC.
    val roc = binarySummary.roc
    roc.show()
    println(binarySummary.areaUnderROC)

    println("----------Decision Tree Classifier------------")

    // Load the data stored in LIBSVM format as a DataFrame.
    val data = ss.read.format("libsvm").load("/home/pi/doc/Spark/spark-2.0.0-bin-hadoop2.7/data/mllib/sample_libsvm_data.txt")

    // Index labels, adding metadata to the label column.
    // Fit on whole dataset to include all labels in index.
    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(data)
    // Automatically identify categorical features, and index them.
    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4) // features with > 4 distinct values are treated as continuous.
      .fit(data)

    // Split the data into training and test sets (30% held out for testing).
    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))

    // Train a DecisionTree model.
    val dt = new DecisionTreeClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")

    // Convert indexed labels back to original labels.
    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

    // Chain indexers and tree in a Pipeline.
    val pipeline = new Pipeline()
      .setStages(Array(labelIndexer, featureIndexer, dt, labelConverter))

    // Train model. This also runs the indexers.
    val model = pipeline.fit(trainingData)

    // Make predictions.
    val predictions = model.transform(testData)

    // Select example rows to display.
    predictions.select("predictedLabel", "label", "features").show(30)

    // Select (prediction, true label) and compute test error.
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)
    println("Test Error = " + (1.0 - accuracy))

    val treeModel = model.stages(2).asInstanceOf[DecisionTreeClassificationModel]
    println("Learned classification tree model:\n" + treeModel.toDebugString)

    println("----------Decision Tree Classifier Test------------")
    //读取数据含有标记和特征的数据
    val decisionTreeData = ss.read.format("libsvm").load("/home/pi/doc/Spark/spark-2.0.0-bin-hadoop2.7/data/mllib/test_decisiontreeclassification_data.txt")
    //转化为ML Index
    val label = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
        .fit(decisionTreeData)
    val feature = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .fit(decisionTreeData)
    //将ML Index 转成String
    val convertor = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictionLabel")
      .setLabels(label.labels)
    //决策树模型
    val dtc = new DecisionTreeClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")
    //将数据随机分为训练集和测试集
    val Array(decisionTrainingData,decisionTestData) = decisionTreeData.randomSplit(Array(0.8,0.2))
    //使用Pipline进行计算
    val decisionPipline = new Pipeline()
        .setStages(Array(label,feature,dtc,convertor))
    //训练决策树模型
    val decisionModel = decisionPipline.fit(decisionTrainingData)
    //使用决策树模型进行预测
    val decisionPredictions = decisionModel.transform(decisionTestData)
    //输出预测结果
    decisionPredictions.select("predictionLabel","label","features").show()
    val decisionTreeModel = decisionModel.stages(2).asInstanceOf[DecisionTreeClassificationModel]
    println("Learned classification tree model:\n" + decisionTreeModel.toDebugString)

    //误差评估
    val decisionEvaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val decisionAccuracy = decisionEvaluator.evaluate(decisionPredictions)
    println("Test Error = " + (1.0 - decisionAccuracy))

    println("----------Random Forest Classifier------------")

    val rfc = new RandomForestClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")
      .setNumTrees(5)
    val randomModel = new Pipeline()
      .setStages(Array(label,feature,rfc,convertor))
      .fit(decisionTrainingData)
    val randomPredictions = randomModel.transform(decisionTestData)
    randomPredictions.select("predictionLabel","label","features").show()
    val randomForestModel = randomModel.stages(2).asInstanceOf[RandomForestClassificationModel]
    println("Learned classification tree model:\n" + randomForestModel.toDebugString)
    //误差评估
    val randomEvaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val randomAccuracy = randomEvaluator.evaluate(randomPredictions)
    println("Test Error = " + (1.0 - randomAccuracy))

    println("----------Random Forest Regression------------")

    val randomForestFeature = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .fit(decisionTrainingData)
    val rfr = new RandomForestRegressor()
      .setLabelCol("label")
      .setFeaturesCol("indexedFeatures")
    val randomForestRegressionPipline = new Pipeline()
      .setStages(Array(randomForestFeature,rfr))
    val rfrModel = randomForestRegressionPipline.fit(decisionTrainingData)
    val rfrPrediction = rfrModel.transform(decisionTestData)
    rfrPrediction.select("prediction","label","features").show()

  }
}
