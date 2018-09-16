import AdjustedRand.findBestK
import RDDTransformations.{getListOfCategoryExistenceString, getProductsWithCategoriesList, getPurchasedCategoriesArrayOfStrings}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD
import Models.ConstrainedKMeans

object Main {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]")
      .setAppName("Targeted Category Promotion").setSparkHome("src/main/resources")
    conf.set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    val transactions = sc.textFile("src/main/resources/groceries.csv")
    val categories = sc.textFile("src/main/resources/products-categorized.csv")

    /** Mapping all products with their corresponding categories - 191 dimensions
      * This RDD contains tuples in the form of :
      *   key:= category
      *   value:= CompactBuffer contain all products corresponding to that category
      * example : Fruits-Vegetables,CompactBuffer(berries, citrus fruit, grapes, herbs, nuts/prunes, onions, other vegetables, packaged, pip fruit, root vegetables, seasonal products, tropical fruit)*/
    val groupedProductsWithCategories = categories.map(x => (x.split(",")(0), x.split(",")(1)))
      .map(x=> (x._1,x._2.split("/").toList))
      .map(x=> getProductsWithCategoriesList(x._1,x._2))
      .flatMap(x=>x.split(" , "))
      .map(x=>(x.split(",")(1).replaceAll("\\)",""),x.split(",")(0).replaceFirst("\\(","")))
      .groupByKey()
      .collect()

    /**  RDD that contains the baskets in their binary vector form,
      *  where we have 1 for categories that we had a purchase in a basket
      *  0 otherwise
      *
      *  The result is NOT A VECTOR, but a string representation of it
      *   example: 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, ... ,jth {0,1}
      *
      *  Each vector is j-dimensional : {0,1} */
    val stringVectorBasketRepresentation = transactions.map(row => getListOfCategoryExistenceString(row.split(",").toList,groupedProductsWithCategories))

    val vectorRDD = stringVectorBasketRepresentation.map(s => Vectors.dense(s.split(",").map(_.toDouble)))


    /** WARNING : The assignment of Customer Ids might take a minute but in a real world dataset where we have the customer
      *   ids we are not in need of that step.
      *
      * Generate and assign random generated customer ids to the baskets.
      * Before assigning the ids to the baskets, we firstly cluster our Dataset with a simple kmeans
      *   and assign the ids based on the clusters created.
      * As a result we have customer with more similar baskets, that way we can create better and more
      *   realistic clusters on our constrained kmeans, and the targeted category promotion will be real and not
      *   completely random.
      *
      * By giving 1000 numberOfCustomers, the GenerateCustomerIds will
      *   : 1) Cluster with Kmeans the baskets to K=40 clusters
      *     2) Assign on those clusters customerIds generated randomly in specific range for each cluster
      *       ex: 1000 numberOfCustomer / 40 clusters = randomly assign 25 unique customerIds to each cluster
      *         for clusterID=0 => assign randomly customerIds in range 1 - 25,
      *             clusterID=1 => assign randomly customerIds in range 26 - 50 and so on ...
      *   */
    val generateCustomerIDs = new GenerateCustomerIDs(k = 40,numberOfCustomer = 1000,vectorRDD)

    /** customerAssignedRDD (vector,int1,int2)
      *   vector : the basket
      *   int1 : rowUniqueIndex
      *   int2 : customerId */
    val customerAssignedRDD = generateCustomerIDs.getCustomerBaskets(vectorRDD)

    /** WARNING, for time simplicity we have commented out the Adjusted Rand oparation, to run it uncomment the next line
      *   and specify the range you want to check, for example from 2 to 50 K.
      * Find best K based on Adjusted Rand, for time reduction the below operation is commented, and used as K
      *   an empirical K that showed most of the times good adjusted rand number */
//    val bestK = findBestK(20,100,1,customerAssignedRDD,sc)
//    println("Best K : "+bestK)

    /** Initialize our ConstrainedKmeans and train it */
    val kmeans = new ConstrainedKMeans(40,5,customerAssignedRDD,1)
    kmeans.train()


    /** Bellow we demonstrate a real incoming basket and how the target category promotion is achieved.
      *   current basket : citrus fruit,semi-finished bread,margarine,ready soups
      *   customer ID : 5 */
    val exampleIncomingBasket=sc.parallelize(Seq("citrus fruit,semi-finished bread,margarine,ready soups"))
    val exampleIncomingBasketStringRepresentation = exampleIncomingBasket.map(row => getListOfCategoryExistenceString(row.split(",").toList,groupedProductsWithCategories))
    exampleIncomingBasketStringRepresentation.foreach(println)
    val exampleIncomingBasketVector = exampleIncomingBasketStringRepresentation.map(s => (Vectors.dense(s.split(",").map(_.toDouble)),5))
    val result = exampleIncomingBasketVector.take(1)

    exampleIncomingBasketVector.foreach(println)
    val predictedCluster = kmeans.predict(result(0)._1,result(0)._2)

    /**frequent itemsets**/
    /** Here after finding the cluster from the incoming basket of the specified customerID,
      *   we now filter all the baskets out of that cluster, and all those baskets will be used to
      *   find frequent itemsets*/
    /** predictedClusterBaskets contains all the baskets from the cluster that the incoming basket is closer or have
      *  a must link connection based on the customerID*/
    val predictedClusterBaskets = kmeans.clusteredBasketsWithNewCentroids.filter(x=>x._5==predictedCluster && !x._3).map(x=>(x._1,x._5))

    /** Example of taking all transactions clustered in cluster 2
      *
      * With getPurchasedCategoriesArrayOfStrings we transforming the vector string representation baskets to their actual purchased categories into
      * their string initial form.
      *
      * Return : List{categories purchased in a basket}
      *   example : List(Yogurt, Fresh-Fruits, Coffee, Dairy-Eggs-Cheese, Yogurt-Pudding, Beverages, Fruits-Vegetables, Tropical-Fruit, Instant-Coffee-Mix)
      */
    val clusterTransactions  : RDD[Array[String]]  = predictedClusterBaskets.map(x=>x._1.toString.replace("[","").replace("]","").replaceAll("1.0","1").replaceAll("0.0","0"))
      .map(basket => getPurchasedCategoriesArrayOfStrings(basket.split(",").toList,groupedProductsWithCategories))

    /** Frequent Itemsets */
    val fpg = new FPGrowth()
      .setMinSupport(0.2)
      .setNumPartitions(10)
    val model = fpg.run(clusterTransactions)

    model.freqItemsets.collect().foreach { itemset =>
      println(itemset.items.mkString("[", ",", "]") + ", " + itemset.freq)
    }

    /** Frequent AssociationRules */
    val minConfidence = 0.2
    model.generateAssociationRules(minConfidence).collect().foreach { rule =>
      println(
        rule.antecedent.mkString("[", ",", "]")
          + " => " + rule.consequent .mkString("[", ",", "]")
          + ", " + rule.confidence)
    }
    /** frequent itemsets **/


  }


  /** Keeps time in ms of what we write in its block */
  def time[R](block: => R): R = {
    val t0 = System.currentTimeMillis()
    val result = block    // call-by-name
    val t1 = System.currentTimeMillis()
    println("Elapsed time: " + (t1 - t0) + "ms")
    result
  }

}
