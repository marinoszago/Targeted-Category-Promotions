import org.apache.spark.mllib.linalg
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.clustering.KMeans
import Models.{ConstrainedKMeans}

class GenerateCustomerIDs(k: Int, numberOfCustomer: Int, vector: RDD[(linalg.Vector)]) extends Serializable  {

  var numberOfCustomers: Int = numberOfCustomer
  var maxUniqueIdsPerCluster: Int = numberOfCustomer/k

  /** assignRandomCUI is a method that given an int (number of cluster) assigns a list of random generated ids
    *   to assign to the baskets, in a way that every cluster will have different assigned randomly generated ids
    *   with all the others. */
  def assignRandomCUI(i: Int):Int = {
    var minCUI :Int = 0
    var maxNumber :Int =0
    if(i==0)
      minCUI = 1
    else
      minCUI = i*maxUniqueIdsPerCluster+1

    maxNumber = minCUI+maxUniqueIdsPerCluster-1

    val rnd = new scala.util.Random
    val number = minCUI+ rnd.nextInt( maxNumber - minCUI)

    number
  }

  def getCustomerBaskets(vector: RDD[(linalg.Vector)]): RDD[(linalg.Vector,Int,Int)] ={
    /** From test we found that the simple Kmeans with that vector gives the best K between 33 and 36,
      *   for time simplicity we reduce the given range to iterate and find the best K based on the Adjusted Rand*/
//    val bestK = findBestK(33,36,vector)
//    println("Best K : "+bestK)
    val kmeans_model = KMeans.train(vector, k, 5, initializationMode = "k-means||", seed = 1)

    val vectorWithIndex = vector.zipWithIndex()
    val basketAndClustersTuples = vectorWithIndex.map{ point =>
      val prediction = kmeans_model.predict(point._1)
      (point._1,prediction.toInt,point._2.toInt)
    }.map( x => (x._1,x._3,assignRandomCUI(x._2)))

    /** (vector,int,int) , vector = basket,  int = row unique index, int = cuid*/
    basketAndClustersTuples
  }

}
