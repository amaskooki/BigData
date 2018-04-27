import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

import org.apache.log4j.Logger
import org.apache.log4j.Level

import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating
import org.dmg.pmml.True
import java.io._


object task1 {

  def get_userID_movieID_rate(line: String): ((Int,Int), Double) ={
    val parts = line.split(",")
    if (parts(0) == "userId") {
      return ((-1, -1), -1)
    }
    else {
      return ((parts(0).toInt, parts(1).toInt), parts(2).toFloat)
    }

  }

  def get_userID_movieID(line: String): ((Int,Int), Double) ={
    val parts = line.split(",")
    if (parts(0) == "userId") {
      return ((-1, -1), -1)
    }
    else {
      return ((parts(0).toInt, parts(1).toInt), -1)
    }

  }


  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)


    val sparkConf = new SparkConf().setAppName("Task1").setMaster("local")
    var sc = new SparkContext(sparkConf)
    sc.setLogLevel("Error")

    val t0 = System.nanoTime()
    val train_file = args(0)
    val test_file = args(1)
    var trainingSettmp = sc.textFile(train_file).map(get_userID_movieID_rate)
    val testingSettmp = sc.textFile(test_file).map(get_userID_movieID)

    var trainingSet = trainingSettmp.subtractByKey(testingSettmp).map(l => Rating(l._1._1,l._1._2,l._2))
    val testingSet = testingSettmp.map{case ((userID, movieID), _) => (userID, movieID)}

    // Build the recommendation model using ALS
    val rank = 10
    val numIterations = 10
    val model = ALS.train(trainingSet, rank, numIterations, 0.5)

    val predictions = model.predict(testingSet).map { case Rating(user, product, rate) =>
        ((user, product), rate)
      }

    val rateActualAndPredict = trainingSettmp.filter( l => l._1._1 != -1 && l._1._2 != -1  ).join(predictions)

    val absErr = rateActualAndPredict.map { case ((user, product), (r1, r2)) =>
      val err = Math.abs(r1 - r2).toInt
      (err,1)
    }.reduceByKey(_+_).collect()

    var errRange = Array(0,0,0,0,0)



    for (i <- 0 until absErr.length){
      if (absErr(i)._1 < 4){
        errRange(absErr(i)._1) = absErr(i)._2
      }
      else{
        errRange(4) = errRange(4) + absErr(i)._2
      }

    }

    for (i <- 0 until errRange.length){
      if (i < 4) {println(">=" + i + " and <"+ (i+1) + ": " + errRange(i))}
      if (i == 4) {println(">=" + i + ": " + errRange(i))}
    }







    val MSE = rateActualAndPredict.map { case ((user, product), (r1, r2)) =>
      val err = (r1 - r2)
      err * err
    }.mean()
    val RMSE = Math.sqrt(MSE)
    println("Mean Squared Error = " + RMSE)

    val t1 = System.nanoTime()




    println("Elapsed time: " + (t1 - t0)*1e-9 + "s")



    val write = predictions.sortBy(_._1._1).map { case ((user,item), rate) =>
      var line = user.toString + "," + item.toString + "," + rate.toString + '\n'
      line
    }.collect()

    val outstring = write.mkString("")
    val out = new PrintWriter("results.txt")
    out.write("UserID,MovieID,Pred_rating\n")
    out.write(outstring)
    out.close

  }


}
