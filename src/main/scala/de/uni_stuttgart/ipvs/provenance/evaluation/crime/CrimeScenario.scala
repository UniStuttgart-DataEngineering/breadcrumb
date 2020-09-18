package de.uni_stuttgart.ipvs.provenance.evaluation.crime

import de.uni_stuttgart.ipvs.provenance.evaluation.{TestConfiguration, TestScenario}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

abstract class CrimeScenario(spark: SparkSession, testConfiguration: TestConfiguration) extends TestScenario(spark, testConfiguration) {

  // path for run in cluster
//  val pathToGeniueDBLP = "/user/hadoop/diesterf/data/dblp/json/"
  // path for run in local
  val pathToGeniueCrime = getPathToGeniueCrime()
  val crimeSchema = getCrimeSchema()
  val personSchema = getPersonSchema()
  val sawpersonSchema = getSawpersonSchema()
  val witnessSchema = getWitnessSchema()

  protected def getPathToGeniueCrime(): String = {
    if (testConfiguration.isLocal) {
      testConfiguration.pathToData
    } else {
      testConfiguration.pathToData + "../"
    }
  }

  def getCrimeSchema() : StructType = {
    val i = spark.read.format("csv").option("header", "true").load(pathToGeniueCrime + "crime.csv")
    i.schema
  }

  def getPersonSchema() : StructType = {
    val p = spark.read.format("csv").option("header", "true").load(pathToGeniueCrime + "person.csv")
    p.schema
  }

  def getSawpersonSchema() : StructType = {
    val a = spark.read.format("csv").option("header", "true").load(pathToGeniueCrime + "sawperson.csv")
    a.schema
  }

  def getWitnessSchema() : StructType = {
    val w = spark.read.format("csv").option("header", "true").load(pathToGeniueCrime + "witness.csv")
    w.schema
  }

  def getPathOffset(): String = {
    if (testConfiguration.isLocal) {
      ""
    } else {
      "_"
    }
  }




  def loadCrime(): DataFrame = {
    val completePath = testConfiguration.pathToData + "/crime"+ getPathOffset() + testConfiguration.getZeros() +"*.csv"
    spark.read.schema(crimeSchema).json(completePath)
  }

  def loadPerson(): DataFrame = {
    val completePath = testConfiguration.pathToData + "/person"+ getPathOffset() + testConfiguration.getZeros() +"*.csv"
    spark.read.schema(personSchema).json(completePath)
  }

  def loadSawperson(): DataFrame = {
    val completePath = testConfiguration.pathToData + "/sawperson"+ getPathOffset() + testConfiguration.getZeros() +"*.csv"
    spark.read.schema(sawpersonSchema).json(completePath)
  }

  def loadWitness(): DataFrame = {
    val completePath = testConfiguration.pathToData + "/witness"+ getPathOffset() + testConfiguration.getZeros() +"*.csv"
    spark.read.schema(witnessSchema).json(completePath)
  }


}
