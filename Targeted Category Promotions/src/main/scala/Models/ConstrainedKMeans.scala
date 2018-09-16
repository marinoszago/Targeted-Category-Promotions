package Models

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

class ConstrainedKMeans(k: Int, iterations: Int, vector: RDD[(linalg.Vector,Int,Int)], seed: Int)
  extends Serializable {

  var totalSizeOfVector: Long = vector.count()-1

  /** flagListOfRGN := Contains a list of tuples with two integers,
    *   First Integer = the ID of Transaction that selected as centroid
    *   Second Integer = the Unique ID for that centroid, takes values from 0 to K-1
    *     Example : for k=5 => our clusters have a unique id from 0 to 4, total 5 clusters ie. centroids */
  val flagListOfRGN = new ListBuffer[Int]()
  var flagListOfCID: Int = -1
  var centroidIndexes: List[(Int, Int)] = _

  var clusteredBasketsWithNewCentroids : RDD[(linalg.Vector,Int,Boolean,Int,Int,Int,ListBuffer[Int])] = _


  /** constraintsList :  contains a ListBuffer that consists of our Constraints
    *                    Its a list of int and a ListBuffer(Int) that:
    *                     First int indicates the clusterUID,
    *                     and the listBuffer consists of all customerIDs that clustered and are to be
    *                     constrained to be clustered to that cluster */
  var constraintsList : ListBuffer[(Int, ListBuffer[Int])] = ListBuffer()
  var updatedConstraintList : ListBuffer[(Int, ListBuffer[Int])] = ListBuffer()
  var currentConstraintList : ListBuffer[(Int)] = ListBuffer()


  /***/
  var centroidTransactions : Array[(linalg.Vector,Int,Boolean,Int,Int,ListBuffer[Int])] = _

  /** initialize of clusterUIDS from 0 to k and empty Lists*/
  for (i <- 0 until k)
    constraintsList.+=((i,new ListBuffer[Int]()))



  /** train : the train method of our ConstrainedKMeans class
    *
    * */
  def   train(): Unit = {


    /**  RDDWithClusters contains :
      *   1) the basket's Vector (linalg.Vector)
      *   2) unique basket's ID (Int)
      *   3) isCentroid (Boolean)
      *   4) an int that specifies if this vector is a centroid, from which cluster its the centroid (value : 0 to k-1),
      *      if the vector it actually belongs to a basket then it has the value of -1 */
    val RDDWithClusters: RDD[(linalg.Vector, Int, Boolean, Int, Int, ListBuffer[Int])] = createClusters(k,vector)

    centroidTransactions = RDDWithClusters.filter(x=>x._3.equals(true)).collect()

    /**  clusteredBaskets is the main Kmeans RDD that contains :
      *   1) the basket's Vector (linalg.Vector)
      *   2) unique basket's ID (Int)
      *   3) isCentroid (Boolean)
      *   4) an int that specifies if this vector is a centroid, from which cluster its the centroid (value : 0 to k-1),
      *      if the vector it actually belongs to a basket then it has the value of -1
      *   5) the unique identifier of a cluster that the basket clustered to, -1 if the row is an actual centroid
      *   6) customer ID
      *   7) init the List(int) that will fill the customer ids constraint */
    clusteredBasketsWithNewCentroids = clusterBaskets(RDDWithClusters)


    /** Now that the initial customer baskets clustered, we are creating the final list(Int) in such a way that
      *   we will ensure basket constrains in the iterations
      *
      *   We populate updatedConstraintList, that will contains our clusterId,List of customer clustered inside
      *     and will be used inside our iterations */
    initClusterConstraints(clusteredBasketsWithNewCentroids)

    var i = 0
    var maxKID = k-1
    var clusterID = 0

    while(i<iterations){
      /** clusteredBasketsWithNewCentroids updates the cluster's centroids to their new average calculated based
        *   on the baskets located to that cluster */
      clusteredBasketsWithNewCentroids = calculateNewCentroids(clusteredBasketsWithNewCentroids,maxKID)

      val centroidTransactions = clusteredBasketsWithNewCentroids.filter(x=>x._3.equals(true)).collect()

      /** now we re assign the baskets based on their new jaccard distances calculated on the new centroids */
      clusteredBasketsWithNewCentroids = clusteredBasketsWithNewCentroids
        .map(x=>(x._1,x._2,x._3,x._4,findClusterWithMinimumDistanceIterative(x._1,x._3,centroidTransactions,x._2,x._6),x._6,x._7))

      i = i + 1
    }

  }

  /** init our cluster constraints from the clustering of rdd */
  def initClusterConstraints(rdd: RDD[(linalg.Vector, Int, Boolean, Int, Int, Int, ListBuffer[Int])]): Unit ={
    rdd.cache()
    var i=0
    do{

      var currentListBuffer :ListBuffer[Int] = ListBuffer()
      val currentClusterCustomerIds = rdd.filter(x=>x._5==i).map(x=>x._6).distinct().collect()
      currentClusterCustomerIds.foreach(x=> {
        currentConstraintList.+=(x)
        }
      )

      updatedConstraintList.+=((i,currentConstraintList))
      currentConstraintList = ListBuffer()
      i+=1
    }while(i<k)

  }

  /** Re calculate the centroids based on the mean of the clustered baskets */
  def calculateNewCentroids(clusteredBaskets :  RDD[(linalg.Vector,Int,Boolean,Int,Int,Int,ListBuffer[Int])], maxKID : Int): RDD[(linalg.Vector,Int,Boolean,Int,Int,Int,ListBuffer[Int])] ={
    clusteredBaskets.cache()
    /** listOfChangedCentroids is a tuple that contains :
      *   an Array and an int,
      *   corresponding to the changed average centroid (j-dimensional array)
      *   and the clusterID that its the centroid (int) */
    val listOfChangedCentroids = new ListBuffer[(Array[(Double)],Int)]()
    for(j<-0 to maxKID){

      /** currentClusterRows contains all the tuples from the current j, where j = 0 to K-1 and
        *   represent the clusterIDs */
      val currentClusterRows = clusteredBaskets.filter(x => x._5==j).map(x=>x._1.toArray)

      listOfChangedCentroids += getMeanVector(currentClusterRows,j,currentClusterRows.count().toInt)

    }

    /** changedCentroids is the new RDD that contains all the clusters but with their centroids changed appropriately
      *   to their new average one. */
    val changedCentroids = clusteredBaskets.map(x=>changeCentroid(x._1,x._2,x._3,x._4,x._5,getSpecificChangedCentroid(x._1,listOfChangedCentroids,x._4),x._6,x._7))

    changedCentroids
  }


  /** Finds and returns the changed centroid in a form of Array(Double),
    *   if the basket is not a centroid, it wont be linked to any element in the
    *   arrayOfChangedCentroids and as a result will return the unchanged basket vector */
  def getSpecificChangedCentroid(currentBasketVector : linalg.Vector,
                                 //                                 isCurrentRowBasketCentroid : Boolean,
                                 arrayOfChangedCentroids : ListBuffer[(Array[(Double)],Int)],
                                 clusterId : Int):  Array[(Double)] = {
    for(elem <- arrayOfChangedCentroids){
      if(elem._2==clusterId)
        return elem._1
    }
    currentBasketVector.toArray

  }

  /** finds the centroids and updates them with their new average basket,
    *   arrayOfChangedCentroid is the average new basket (ie the new centroid for that cluster)*/
  def changeCentroid(currentRowBasket : linalg.Vector,
                     currentRowId : Int,
                     isCurrentRowBasketCentroid : Boolean,
                     clusterFlag: Int, /** -1 if the basket isnt a centroid, clusterUID otherwise (0 to k-1) */
                     clusterID: Int, /** clusterUID that the basket belongs */
                     arrayOfChangedCentroid : Array[(Double)],
                     customerId : Int,
                     list :ListBuffer[Int])
  :(linalg.Vector, Int, Boolean, Int, Int, Int, ListBuffer[Int])={

    if(isCurrentRowBasketCentroid){
      val result =  Vectors.dense(arrayOfChangedCentroid)
      (result,currentRowId,isCurrentRowBasketCentroid,clusterFlag,clusterID,customerId,list)

    } else
      (currentRowBasket,currentRowId,isCurrentRowBasketCentroid,clusterFlag,clusterID,customerId,list)

  }

  /** getMeanVector is calculating the the new centroid of the given clusterUID based on the baskets it contains */
  def getMeanVector(clusteredBaskets : RDD[Array[(Double)]], clusterUID : Int, numberOfBaskets : Int): (Array[(Double)],Int) ={
    try{
      val basketsAdded = clusteredBaskets.reduce{
        (x,y) =>{
          val vector = addVectors(x,y)
          vector
        }
      }

      val basketsMean = basketsAdded.map(x=>x/numberOfBaskets)

      (basketsMean,clusterUID)
    }catch{
      case e: Exception =>{
        println(e)
        null
      }
    }

  }

  /** addVectors is a method that aggregate point to point two vectors, used to calculate the average new centroids */
  def addVectors(vector1:Array[(Double)], vector2:Array[(Double)]): Array[(Double)]={
    val result = (vector1, vector2).zipped.map(_ + _)

    result
  }

  /** createClusters is a method that takes as input number of centroids to generate, and it maps a vector in form of
    * (vector basket, row unique ID, customerID) to => (vector basket, row unique ID, is centroid, clusterUID, customerID)
    *   where is centroid = true when the specified row is actual centroid generated randomly,
    *   or centroid = false otherwise
    *
    *   As a result we have an RDD that contains N-K non centroid baskets, and K centroid baskets */
  def createClusters(k : Int, vector: RDD[(linalg.Vector,Int,Int)]): RDD[(linalg.Vector,Int,Boolean,Int,Int,ListBuffer[Int])] =
  {
    /** initCentroidIndexes : choose k random baskets from our RDD */
    initCentroidIndexes(k)
    val vectorWithCentroids = vector.map(x=>(x._1,x._2,isCluster(x._2,centroidIndexes),getClusterId(x._2,centroidIndexes),x._3,new ListBuffer[Int]))

    vectorWithCentroids
  }

  /** initCentroidIndexes is a method that gets the number of clusters K, and choose randomly in our list of baskets k
    *   rows that will be our initial centroids of our cluster model */
  def initCentroidIndexes(k : Int): Unit={

    centroidIndexes = List.tabulate(k)(n => getRandomNumber(0,totalSizeOfVector.toInt))

  }

  /** clusterBaskets clusters the baskets to the closest centroids aka clusters */
  def clusterBaskets(RDDWithClusters: RDD[(linalg.Vector, Int, Boolean, Int, Int,ListBuffer[Int])])
  : RDD[(linalg.Vector, Int,Boolean, Int, Int, Int, ListBuffer[Int])] ={

    val inputTransactions = RDDWithClusters
      .map(x=>(x._1,x._2,x._3,x._4,findClusterWithMinimumDistance(x._1,x._3,x._2,x._5,centroidTransactions),x._5,x._6))

    inputTransactions
  }


  /** findClusterWithMinimumDistance a method that get as input
    *     the currentRowBasket we need to cluster,
    *     the second is the currentRowBasket Boolean value that indicates if it is a centroid,
    *     the third is an array of our K centroid vectors (K clusters) and their clusterUID,
    *     at forth we have our currentRowBasket UID,
    *     lastly we have our Customer ID
    *
    * For each element in the second array that contains the centroid vectors, we find its jaccard distance if and only if
    *   the currentRowBasket is not a centroid with the currentRowBasket vector
    *
    * Result := the number of the cluster that contains the centroid that has the minimum jaccard distance with the
    *   current row basket vector, or -1 if the currentRowBasket is a centroid*/
  def findClusterWithMinimumDistance(currentRowBasket : linalg.Vector,
                                     isCurrentRowBasketCentroid : Boolean,
                                     currentRowId: Int,
                                     customerID: Int,
                                     centroidTransactions : Array[(linalg.Vector, Int, Boolean, Int, Int,ListBuffer[Int])]): Int ={
    /** distanceResults contains the tuples that consists of
      *   Double : jaccard distance found between currentBasket and centroid
      *   Int : centroid's clusterUID that the calculated distance based on */
    var distanceResults = ArrayBuffer[(Double, Int)]()

    if(isCurrentRowBasketCentroid.equals(true)){
      return getClusterId(currentRowId,centroidIndexes)
    } else {
      for (elem <- centroidTransactions) {
        distanceResults += jaccardDistance(elem._1,currentRowBasket,elem._4)
      }
    }

    /** constraintExist a method that checks if a given customerID is already inside a cluster,
      *   if it is then this method returns the clusterID the customer is assigned and have a must-link constraint
      *   -1 otherwise*/
    val constraint = constraintExist(customerID)
    if(constraint!=(-1)){
      return constraint
    }

    /** finds the closer cluster based on the populated distanceResults Array */
    val clusterUIDWithMinimumDistance = findCloserCluster(distanceResults)

    /** updates the constraint list */
    updateConstraintList(clusterUIDWithMinimumDistance,customerID)

    /** the cluster for the current row to be assigned */
    clusterUIDWithMinimumDistance
  }

  def findClusterWithMinimumDistanceIterative(currentRowBasket : linalg.Vector,
                                     isCurrentRowBasketCentroid : Boolean,
                                     centroidTransactions: Array[(linalg.Vector, Int, Boolean, Int, Int, Int, ListBuffer[Int])],
                                     currentRowId: Int,
                                     customerID: Int): Int ={
    /** distanceResults contains the tuples that consists of
      *   Double : jaccard distance found between currentBasket and centroid
      *   Int : centroid's clusterUID that the calculated distance based on */
    var distanceResults = ArrayBuffer[(Double, Int)]()

    if(isCurrentRowBasketCentroid.equals(true)){
      return getClusterId(currentRowId,centroidIndexes)
    } else {
      for (elem <- centroidTransactions) {
        distanceResults += jaccardDistance(elem._1,currentRowBasket,elem._4)
      }
    }

    val constraint = constraintExistIterative(customerID)
    if(constraint!=(-1)){
      return constraint
    }

    val clusterUIDWithMinimumDistance = findCloserCluster(distanceResults)

    updateConstraintList(clusterUIDWithMinimumDistance,customerID)

    clusterUIDWithMinimumDistance
  }

  /** Searches the list of constraints, if the customerId is found in any of the clusters lists, then the id of that
    *   cluster is returned
    *
    * Return : clusterID that its list with customer ids contains the current customer id that is to be clustered
    *          -1 otherwise, if there is no cluster with such customer */
  def constraintExist(customerID: Int): Int={
    var i = 0
    do{
      if(constraintsList(i)._2.contains(customerID))
        return constraintsList(i)._1
      i+=1
    }while(i<k)

    -1
  }

  def constraintExistIterative(customerID: Int): Int={
    var i = 0
    do{
      if(updatedConstraintList(i)._2.contains(customerID))
        return updatedConstraintList(i)._1
      i+=1
    }while(i<k)

    -1
  }

  /** updateConstraintList assigns a customerID to the list of given cluster constraints */
  def updateConstraintList(clusterUIDWithMinimumDistance:Int, customerID: Int): Unit={
    constraintsList(clusterUIDWithMinimumDistance)._2.+=(customerID)

  }

  /** Closer to 0 => more equality */
  def findCloserCluster(distanceResults :ArrayBuffer[(Double, Int)]): Int={

    val closerCluster = getMin(distanceResults)

    closerCluster._2
  }

  /** getMin is a method that accepts an ArrayBuffer that contains all tuples which consists of
    *   (Double,Int) where Double is the distance and Int is the number of Cluster (Identifier)
    *
    *   Return the tuple that has the minimum distance meaning the closer */
  def getMin[B >: A, A](xs: Iterable[A])(implicit cmp: Ordering[B]): (A) = {
    if (xs.isEmpty) throw new UnsupportedOperationException("empty.min")

    val initial = xs.head

    xs.foldLeft(initial) { case ((min), x) =>
      if (cmp.lt(x, min))
        x
      else
        min
    }
  }

  /** jaccardDistance returns a tuple of distance,clusterID, where distance is the jaccard distance corresponding
    *   between centroid vector and a given basket vector */
  def jaccardDistance(centroidVector : linalg.Vector, currentBasketVector : linalg.Vector, cluster: Int):(Double, Int) ={

    var count = 0
    var M11 = 0
    var M01 = 0
    var M10 = 0

    val currentBasketList = currentBasketVector.toArray
    val centroidVectorList = centroidVector.toArray
    val arrayLength = currentBasketList.length

    var i = 0
    while (i < arrayLength){

      if(centroidVectorList(i)==1 && currentBasketList(i)==1)
        M11 +=1
      else if (centroidVectorList(i)==0 && currentBasketList(i)==1)
        M01 +=1
      else if (centroidVectorList(i)==1 && currentBasketList(i)==0)
        M10 +=1

      i += 1
      count +=1
    }

    val result: Double = (M01 + M10).toFloat / (M11 + M01 + M10).toFloat

    (result,cluster)
  }


  /** getRandomNumber is a method that generated a random number between 0 and sizeOfRDD-1, returns a list of distinct
    * indexes in that range of numbers that shows which baskets from our vector will be our initialize centroids  */
  def getRandomNumber(start:Int, end:Int): (Int,Int) = {
    var number = 0
    var clusterUID = 0
    do{
      val rnd = new scala.util.Random()
      number = rnd.nextInt( end - start  )
    }while(flagListOfRGN.contains(number))
    flagListOfRGN += number
    flagListOfCID += 1

    (number,flagListOfCID)
  }


  def isCluster(rid: Int, cendroidIndexes: List[(Int,Int)]): Boolean = {
    if(flagListOfRGN.contains(rid))
      true
    else
      false

  }

  /** getClusterId gets as input rid that is the current tuple's row ID,
    *   and our List that contains a map between our randomGeneratedTupleID, ClusterUniqueID
    *
    * Returns -1 if the current rowID is not a certain centroid,
    *   or a number between 0 to k-1 that is the unique cluster's identifier */
  def getClusterId(rid: Int, cendroidIndexes: List[(Int,Int)]): Int = {

    for(elem <- cendroidIndexes){
      if(elem._1==rid)
        return elem._2
    }

    -1
  }

  /** predict : a method that gets a vector basket and customerID and returns in what cluster it belongs */
  def predict(point: linalg.Vector, customerID :Int): Int ={
    var distanceResults = ArrayBuffer[(Double, Int)]()

    val constraint = constraintExistIterative(customerID)
    if(constraint!=(-1)){
      return constraint
    }

    for (elem <- centroidTransactions) {
      distanceResults += jaccardDistance(elem._1,point,elem._4)
    }

    val clusterUIDWithMinimumDistance = findCloserCluster(distanceResults)

    clusterUIDWithMinimumDistance
  }


}
