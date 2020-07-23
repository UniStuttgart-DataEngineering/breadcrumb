package de.uni_stuttgart.ipvs.provenance.evaluation.twitter

import de.uni_stuttgart.ipvs.provenance.evaluation.{TestConfiguration, TestScenario}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

abstract class TwitterScenario (spark: SparkSession, testConfiguration: TestConfiguration) extends TestScenario (spark, testConfiguration) {


  val twitterSchema = getTwitterSchema()

  def getTwitterSchema() : StructType = {
    val t = spark.read.json(testConfiguration.pathToData + "08_09_17_00_stream000000.log")
    t.schema
  }

  def loadTweets(): DataFrame = {
    val completePath = testConfiguration.pathToData + "/*" + testConfiguration.getZeros() + ".log"
    spark.read.schema(twitterSchema).json(completePath)
  }







}
