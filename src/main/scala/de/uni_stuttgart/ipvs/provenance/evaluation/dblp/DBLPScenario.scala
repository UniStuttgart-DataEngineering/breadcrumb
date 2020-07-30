package de.uni_stuttgart.ipvs.provenance.evaluation.dblp

import de.uni_stuttgart.ipvs.provenance.evaluation.{TestConfiguration, TestScenario}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType

abstract class DBLPScenario(spark: SparkSession, testConfiguration: TestConfiguration) extends TestScenario(spark, testConfiguration) {

  // path for run in cluster
//  val pathToGeniueDBLP = "/user/hadoop/diesterf/data/dblp/json/"
  // path for run in local
  val pathToGeniueDBLP = getPathToGeniueDBLP()
  val inproceedingsSchema = getInproceedingsSchema()
  val proceedingsSchema = getProceedingsSchema()
  val articleSchema = getArticleSchema()
  val wwwSchema = getWWWSchema()

  import spark.implicits._

  protected def getPathToGeniueDBLP(): String = {
    if (testConfiguration.isLocal) {
      testConfiguration.pathToData
    } else {
      testConfiguration.pathToData + "../"
    }
  }

  def getInproceedingsSchema() : StructType = {
    val i = spark.read.json(pathToGeniueDBLP + "inproceedings.json")
    i.schema
  }

  def getProceedingsSchema() : StructType = {
    val p = spark.read.json(pathToGeniueDBLP + "proceedings.json")
    p.schema
  }

  def getArticleSchema() : StructType = {
    val a = spark.read.json(pathToGeniueDBLP + "article.json")
    a.schema
  }

  def getWWWSchema() : StructType = {
    val w = spark.read.json(pathToGeniueDBLP + "www.json")
    w.schema
  }

  def getPathOffset: String = {
    if (testConfiguration.isLocal) {
      "_"
    } else {
      ""
    }
  }




  def loadInproceedings(): DataFrame = {
    val completePath = testConfiguration.pathToData + "/inproceedings"+ getPathOffset() + testConfiguration.getZeros() +"*.json"
    spark.read.schema(inproceedingsSchema).json(completePath)
  }

  def loadProceedings(): DataFrame = {
    val completePath = testConfiguration.pathToData + "/proceedings*"+ getPathOffset() + testConfiguration.getZeros() +"*.json"
    spark.read.schema(proceedingsSchema).json(completePath)
  }

  def loadArticle(): DataFrame = {
    val completePath = testConfiguration.pathToData + "/article"+ getPathOffset() + testConfiguration.getZeros() +"*.json"
    spark.read.schema(articleSchema).json(completePath)
  }

  def loadWWW(): DataFrame = {
    val completePath = testConfiguration.pathToData + "/www"+ getPathOffset() + testConfiguration.getZeros() +"*.json"
    spark.read.schema(wwwSchema).json(completePath)
  }


}
