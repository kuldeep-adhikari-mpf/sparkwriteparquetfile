package sql

import org.apache.spark
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.ml.feature.{OneHotEncoderEstimator, StringIndexer, VectorAssembler}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.Pipeline
import org.apache.log4j._

object TrainModelAndPredict {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .config("spark.master", "local")
      .getOrCreate()
    //load training data
    val data=loadTestData(spark)
    data.show()
    /*A Spark model needs exactly two columns: “label” and “features”. To get there will take a few steps. First we will identify our
  label using the select method while also keeping only relevant columns
   */
    /* predict gender as Male or Female, In the above model you pass features like Hobbies, color like etc.
     So after computing, it will return the gender as Male or Female. That's called a Label*/
    val dfLabels=getDFLabels(data)
    dfLabels.show()
    /* we will do some one-hot encoding on our categorical features. This takes a few steps. First we have to use the StringIndexer to convert the strings to integers.
Then we have to use the OneHotEncoderEstimator to do the encoding.
*/
    val encoded=convertStringLabelToInteger(dfLabels)
    encoded.show()
    /*Next we check for null values. In this dataset I was able to find the number of null values through some relatively simple code,
  though depending on the data it may be more complicated:
  After checking the columns, I decided to impute the null values of the following columns
  using the median value of that column: nevents, ndays_act, nplay_video, nchapters. I did this like so:
   */
    val filled=convertNullValues(encoded)
    filled.show()
    /*Then we use the VectorAssembler object to construct our “features” column. Remember, Spark models need exactly two columns: “label” and “features”.
   */
    val output=getDFFeatures(filled)
    output.show()
    // Splitting the data by create an array of the training and test data
    val Array(training, test) = output.select("label","features").randomSplit(Array(0.7, 0.3), seed = 12345)

    /*The data is set up! Now we can create a model object (I’m using a Random Forest Classifier), define a parameter grid (I kept it simple and only
     varied the number of trees), create a Cross Validator object (here is where we set our scoring metric for training the model) and fit the model.
      */
    val model=createRandomForestModel(training)
    println("Now we have a trained, cross validated model! ")
    /*It’s time for some model evaluation. This is a little more difficult because the evaluation functionality still mostly resides in the RDD-API for Spark,
     requiring some different syntax. Let’s begin by getting predictions on our test data and storing them.
      */
    val results = model.transform(test).select("features", "label", "prediction")
    results.show()
    //We will then convert these results to an RDD.
    import spark.implicits._
    val predictionAndLabels = results.
      select("prediction","label").
      as[(Double, Double)].
      rdd

   // Then we can create our metrics objects and print out the confusion matrix.
   // Instantiate a new metrics objects
   val bMetrics = new BinaryClassificationMetrics(predictionAndLabels)
    val mMetrics = new MulticlassMetrics(predictionAndLabels)
    val labels = mMetrics.labels

    // Print out the Confusion matrix
    println("Confusion matrix:")
    println(mMetrics.confusionMatrix)
    spark.stop()

  }

  //load training data
  def loadTestData(spark: SparkSession):DataFrame = {
     spark.read.option("header", "true").
      option("inferSchema", "true").
      format("csv").
      load("/Users/kuldeep.adhikari/IdeaProjects/sparkwriteparquetfile/TestData/TrainingDataToIdentifyGender")
  }

  /*A Spark model needs exactly two columns: “label” and “features”. To get there will take a few steps. First we will identify our
  label using the select method while also keeping only relevant columns
   */
 /* predict gender as Male or Female, In the above model you pass features like Hobbies, color like etc.
  So after computing, it will return the gender as Male or Female. That's called a Label*/

  def getDFLabels(data: DataFrame): DataFrame = {
   // data.select(data("gender").as("label"),$"hobbies",$"color",$"age")
    data.select("gender","hobbies","color","age")
  }


  /* we will do some one-hot encoding on our categorical features. This takes a few steps. First we have to use the StringIndexer to convert the strings to integers.
 Then we have to use the OneHotEncoderEstimator to do the encoding.
 */

  def convertStringLabelToInteger(dfLabels: DataFrame) :DataFrame={
    // string indexing
    val indexer1 = new StringIndexer().
      setInputCol("gender").
      setOutputCol("label").
      setHandleInvalid("keep")
    val indexed1 = indexer1.fit(dfLabels).transform(dfLabels)

     val indexer2 = new StringIndexer().
      setInputCol("hobbies").
      setOutputCol("hobbiesIndex").
      setHandleInvalid("keep")
    val indexed2 = indexer2.fit(indexed1).transform(indexed1)

    val indexer3 = new StringIndexer().
      setInputCol("color").
      setOutputCol("colorIndex").
      setHandleInvalid("keep")
    val indexed3 = indexer3.fit(indexed2).transform(indexed2)

    // one hot encoding
    val encoder = new OneHotEncoderEstimator().
      setInputCols(Array("hobbiesIndex","colorIndex")).
      setOutputCols(Array("hobbiesVec","colorVec"))
    val encoded = encoder.fit(indexed3).transform(indexed3)
    encoded

  /* val indexer1 = new StringIndexer().
   setInputCol("hobbies").
   setOutputCol("hobbiesIndex").
   setHandleInvalid("keep")
    val indexed1 = indexer1.fit(dfLabels).transform(dfLabels)
    val indexer2 = new StringIndexer().
      setInputCol("color").
      setOutputCol("colorIndex").
      setHandleInvalid("keep")
    val indexed2 = indexer2.fit(indexed1).transform(indexed1)

    val encoder = new OneHotEncoderEstimator().
      setInputCols(Array( "hobbiesIndex","colorIndex")).
      setOutputCols(Array("hobbiesVec","colorVec"))
    val encoded = encoder.fit(indexed2).transform(indexed2)
    encoded*/

  }

  /*Next we check for null values. In this dataset I was able to find the number of null values through some relatively simple code,
  though depending on the data it may be more complicated:
  After checking the columns, I decided to impute the null values of the following columns
  using the median value of that column: nevents, ndays_act, nplay_video, nchapters. I did this like so:
   */
  def convertNullValues(encoded: DataFrame) :DataFrame={
   /* val nanEvents = encoded.groupBy("gender").count().orderBy($"count".desc)
    for (line <- nanEvents){
      println(line)
    }
    // define medians
    val neventsMedianArray = encoded.stat.approxQuantile("nevents", Array(0.5), 0)
    val neventsMedian = neventsMedianArray(0)

    val ndays_actMedianArray = encoded.stat.approxQuantile("ndays_act", Array(0.5), 0)
    val ndays_actMedian = ndays_actMedianArray(0)

    val nplay_videoMedianArray = encoded.stat.approxQuantile("nplay_video", Array(0.5), 0)
    val nplay_videoMedian = nplay_videoMedianArray(0)

    val nchaptersMedianArray = encoded.stat.approxQuantile("nchapters", Array(0.5), 0)
    val nchaptersMedian = nchaptersMedianArray(0)

    // replace
    val filled = encoded.na.fill(Map(
      "nevents" -> neventsMedian,
      "ndays_act" -> ndays_actMedian,
      "nplay_video" -> nplay_videoMedian,
      "nchapters" -> nchaptersMedian))*/
    encoded
  }



  /*Then we use the VectorAssembler object to construct our “features” column. Remember, Spark models need exactly two columns: “label” and “features”.
   */
  def getDFFeatures(filled: DataFrame):DataFrame = {
    // Set the input columns as the features we want to use
    val assembler = (new VectorAssembler().setInputCols(Array(
      "age","hobbiesVec", "colorVec")).
      setOutputCol("features"))

    // Transform the DataFrame
  //  val output = assembler.transform(filled).select("labelIndex","features")
  val output = assembler.transform(filled).select("label","features")
    output

  }


  def createRandomForestModel(training: DataFrame):CrossValidatorModel={
    // create the model
    val rf = new RandomForestClassifier()

    // create the param grid
    val paramGrid = new ParamGridBuilder().
      addGrid(rf.numTrees,Array(20,50,100)).
      build()

    // create cross val object, define scoring metric
    val cv = new CrossValidator().
      setEstimator(rf).
      setEvaluator(new MulticlassClassificationEvaluator().setMetricName("weightedRecall")).
      setEstimatorParamMaps(paramGrid).
      setNumFolds(3).
      setParallelism(2)

    // You can then treat this object as the model and use fit on it.
    val model = cv.fit(training)
    model
  }



}
