import scala.collection.mutable.ListBuffer

object RDDTransformations {


  def getListOfCategoryExistenceString(currentRowProducts: List[String], distinctCategoriesWithProducts: Array[(String,Iterable[(String)])] ) : String ={
    val listOfCategories = new ListBuffer[Array[Int]]()

    for(product <- currentRowProducts){
      val foundCategories = distinctCategoriesWithProducts.map(x => categoryExist(product,x))
      listOfCategories += foundCategories
    }

    var combinedListOfCategories = List.fill(distinctCategoriesWithProducts.length)(0)
    for(list <- listOfCategories){
      combinedListOfCategories =  combinedListOfCategories.zipAll(list, 0, 0).map {
        case (a, b) => {
          if((a + b) > 0){
            1
          }

          else
            0
        }

      }
    }

    val categoryPurchases = combinedListOfCategories.toString.replaceAll("List\\(","").replaceAll("\\)","")

    categoryPurchases
  }

  def getPurchasedCategoriesArrayOfStrings(currentBasketVector: List[String], distinctCategoriesWithProducts: Array[(String,Iterable[(String)])] ) : Array[(String)] ={

    val listOfCategories = distinctCategoriesWithProducts.toList
    var combinedListOfCategories = List.fill(distinctCategoriesWithProducts.length)("")

    combinedListOfCategories =  listOfCategories.zipAll(currentBasketVector, 0, 0).map {
      case (a, b) => {
        if (b.toString.trim.toInt > 0) {
          a.toString.split(",")(0).replaceAll("\\(","")
        }
        else
          ""
      }
    }.filterNot(x=>x.equals(""))

    combinedListOfCategories.toArray

  }

  /** categoryExist := splits the current row products by ",", and search in our j-dimensional category list if contains
    * any product and return 1 if it does or 0 otherwise. */
  def categoryExist(currentProducts: String, currentCategoryProducts: (String,Iterable[(String)])): Int ={
    var listOfCategories = currentCategoryProducts._2.toString
    val result = currentCategoryProducts._2.toString.contains(currentProducts)
    if(result.equals(true))
      1
    else
      0
  }

  def getProductsWithCategoriesList(product: String, listOfCategoriesWithProducts: List[String]): String = {

    var listOfCombinations =  new StringBuilder

    for (category <- listOfCategoriesWithProducts)
      listOfCombinations.append("("+product+","+category.trim()+") , ")

    listOfCombinations.toString()
  }

}
