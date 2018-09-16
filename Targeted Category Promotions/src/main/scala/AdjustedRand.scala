import DAO.FileWritter
import Models.ConstrainedKMeans
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.clustering.KMeans
object AdjustedRand{




  /** given a range find the best k that based on the adjusted rand cluster quality evaluation
    *   this method is for our implementation of constrained kmeans and is used for finding the best k */
  def findBestK(lowerRangeK : Int, maximumRangeK : Int, numOfIterations : Int, customerAssignedRDD : RDD[(org.apache.spark.mllib.linalg.Vector,Int,Int)]): Int ={
    val fileWritter = new FileWritter()

    customerAssignedRDD.cache()
    var K = 0
    var currentK = lowerRangeK
    var previousARI = 0.0

    while(currentK < maximumRangeK){

      val currentModel = new ConstrainedKMeans(currentK,numOfIterations,customerAssignedRDD, seed = 1)
      currentModel.train()

      val transactionsAndClusterIdx1 = customerAssignedRDD.map{ point =>
        val prediction = currentModel.predict(point._1,point._3)
        prediction.toInt
      }.zipWithUniqueId().map( x => (x._2,x._1))

      currentK += 1
      val nextModel = new ConstrainedKMeans(currentK,numOfIterations,customerAssignedRDD, seed = 1)
      nextModel.train()

      val transactionsAndClusterIdx2 = customerAssignedRDD.map{ point =>
        val prediction = nextModel.predict(point._1,point._3)
        prediction.toInt
      }.zipWithUniqueId().map( x => (x._2,x._1))

      var currentARI = adjustedRandIndex(transactionsAndClusterIdx1,transactionsAndClusterIdx2,currentK-1,currentK)
      println("Model1 k :"+(currentK-1)+", Model2 k:"+currentK+", ARI result : "+currentARI)
      println("CurrentARI :"+ currentARI +", PreviousARI :"+previousARI+", c-p : "+(currentARI-previousARI))

      fileWritter.appendFile("src/main/resources/results.txt","Model1 k :"+(currentK-1)+", Model2 k:"+currentK+", ARI result : "+currentARI+".\n")

      if(currentARI >= 0.0 && currentARI < previousARI){
        println("Model1 k :"+(currentK-1)+", Model2 k:"+currentK+", ARI result : "+currentARI)
        return currentK-1
      }
      K = currentK
      previousARI = currentARI
    }

    K
  }

  /** given a range find the best k that based on the adjusted rand cluster quality evaluation
    *   this method is for the simple kmean and is used for finding the best k for the first step of clustering
    *   to assign the customerIDs properly */
  def findBestK(lowerRangeK : Int, maximumRangeK : Int, vectorRDD : RDD[org.apache.spark.mllib.linalg.Vector]): Int ={
    val numIterations = 10
    var K = 0
    var currentK = lowerRangeK
    var previousARI = 0.0

    while(currentK < maximumRangeK){

      val currentModel = KMeans.train(vectorRDD, currentK, numIterations, initializationMode = "k-means||", seed = 1)

      val transactionsAndClusterIdx1 = vectorRDD.map{ point =>
        val prediction = currentModel.predict(point)
        prediction.toInt
      }.zipWithUniqueId().map( x => (x._2,x._1))

      currentK += 1
      val nextModel = KMeans.train(vectorRDD, currentK, numIterations,initializationMode = "k-means||",seed = 1)

      val transactionsAndClusterIdx2 = vectorRDD.map{ point =>
        val prediction = nextModel.predict(point)
        prediction.toInt
      }.zipWithUniqueId().map( x => (x._2,x._1))

      var currentARI = adjustedRandIndex(transactionsAndClusterIdx1,transactionsAndClusterIdx2,currentK-1,currentK)
      println("Model1 k :"+(currentK-1)+", Model2 k:"+currentK+", ARI result : "+currentARI)
      println("CurrentARI :"+ currentARI +", PreviousARI :"+previousARI+", c-p : "+(currentARI-previousARI))

      if(currentARI >= 0.0 && currentARI < previousARI){
        println("Model1 k :"+(currentK-1)+", Model2 k:"+currentK+", ARI result : "+currentARI)
        return currentK-1
      }
      K = currentK
      previousARI = currentARI
    }

    K
  }

  /** calculates the adjusted rand index comparing the results of i-1 models cluster and current i model */
  def adjustedRandIndex(model1Results : RDD[(Long, Int)], model2Results : RDD[(Long, Int)],
                          model1K : Int, model2K : Int): Double = {

    val joinedModels = model1Results.join(model2Results).map(x=>x._2).cache()

    var totalNumberOfTransactions = joinedModels.count()

    var rowCountParagontiko = 0.0
    var columnCountParagontiko = 0.0
    var row_columnCountParagontiko = 0.0
    var i = 0
    var j = 0
    var rowcount = 0
    var colcount = 0

    while (i<model1K){

      while(j<model2K){
        val count = joinedModels.filter(x=>x._1==i && x._2==j).count()
        if(count != 0){
          var currentParagontiko = calculateN_2(count.toInt)
          row_columnCountParagontiko = row_columnCountParagontiko + currentParagontiko
          currentParagontiko = 0
        }
        j += 1
      }

      i += 1
    }
    i = 0
    j = 0

    while(i<model1K){

      while(j<model2K){
        val count = joinedModels.filter(x=>x._1==i && x._2==j).count()
        if(count != 0){
          rowcount = rowcount + count.toInt
        }

        j+=1
      }
      var currentParagontiko = calculateN_2(rowcount)
      rowCountParagontiko = rowCountParagontiko + currentParagontiko
      currentParagontiko = 0
      rowcount = 0
      j = 0
      i += 1
    }

    i = 0
    j = 0

    while(i<model2K){

      while(j<model1K){
        val count = joinedModels.filter(x=>x._1==j && x._2==i).count()
        if(count != 0){
          colcount = colcount + count.toInt

        }
        j += 1
      }

      var currentParagontiko = calculateN_2(colcount)
      columnCountParagontiko = columnCountParagontiko + currentParagontiko
      currentParagontiko = 0
      colcount = 0
      j = 0
      i += 1
    }


    var index = row_columnCountParagontiko.toFloat
    var totalTransactionsN_2 = calculateN_2(totalNumberOfTransactions.toInt)
    var expectedIndex = (rowCountParagontiko * columnCountParagontiko).toFloat / totalTransactionsN_2.toFloat
    var maxIndex = (0.5 * (rowCountParagontiko + columnCountParagontiko)).toFloat

    var numerator = index - expectedIndex
    var denominator = maxIndex - expectedIndex
    var ARI = numerator / denominator


    return ARI
  }

  def calculateN_2(n: Int) : Double = {

    var result = 0.toFloat

    result = (n * (n - 1)).toFloat / 2.toFloat

    result
  }

}
