package de.uni_stuttgart.ipvs.provenance.evaluation

import de.uni_stuttgart.ipvs.provenance.evaluation.dblp.DBLPSuite
import de.uni_stuttgart.ipvs.provenance.evaluation.tpch.TPCHSuite
import de.uni_stuttgart.ipvs.provenance.evaluation.twitter.TwitterSuite
import org.apache.spark.sql.SparkSession

object TestExecution extends App {

  def init(appName: String) : SparkSession = {
    SparkSession
      .builder()
      .config("spark.extraListeners", "de.uni_stuttgart.ipvs.provenance.evaluation.EvaluationListener")
      .appName(appName)
      .getOrCreate()
  }

  def getTestSuite(spark: SparkSession, testConfiguration: TestConfiguration) = {
    args(0) match {
      case "twitter" => TwitterSuite(spark, testConfiguration)
      case "dblp" => DBLPSuite(spark, testConfiguration)
      case "tpch" => TPCHSuite(spark, testConfiguration)
      case _ => throw new MatchError("Testcase is not \"twitter\", \"dblp\" or \"tpch\"")
    }
  }

  def verifyParameters() = {
    if (args.length != 8) {
      println("Usage: ProvenanceEvaluation [twitter|dblp] [true|false] [100|200|300|400|500] [1..100] [true|false] testMask pathToData")
      println("Provided Parameters:")
      args.foreach(println)
      System.exit(-1)
    }
  }

  verifyParameters()

  val spark = init("Provenance_Evaluation")
  val testConfiguration = TestConfiguration(args.tail)
  val testSuite = this.getTestSuite(spark, testConfiguration)
  testSuite.executeScenarios()

}
