package DAO

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag
import scala.reflect.io.File

class FileWritter() extends Serializable {


  def initializeEmptyFile(filePath : String): Unit = {

    /** Initialize to empty text the file that contains the time measurements of different joins */
    Files.write(Paths.get(filePath), "".getBytes(StandardCharsets.UTF_8))

  }

  def appendFile(filePath : String, text : String): Unit = {

    File(filePath).appendAll(text)

  }

  def saveToOutputFile[K : Ordering : ClassTag, V1 : ClassTag, V2 : ClassTag]
  (rddToSave : RDD[(K, (V1,V2))], fileNameToSave : String)={

    val repartitioned = rddToSave.repartition(1).sortByKey(true)
    repartitioned.saveAsTextFile("src/main/resources/"+fileNameToSave)

  }

  def saveToOutputFile2[K : Ordering : ClassTag, V1 : ClassTag]
  (rddToSave : RDD[(K, V1)], fileNameToSave : String)={

    val repartitioned = rddToSave.repartition(1).sortByKey(true)
    repartitioned.saveAsTextFile("src/main/resources/"+fileNameToSave)

  }

}
