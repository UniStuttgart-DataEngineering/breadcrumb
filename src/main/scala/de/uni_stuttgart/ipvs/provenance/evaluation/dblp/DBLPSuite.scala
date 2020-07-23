package de.uni_stuttgart.ipvs.provenance.evaluation.dblp


import de.uni_stuttgart.ipvs.provenance.evaluation.{TestConfiguration, TestScenario, TestSuite}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.DataFrame

object DBLPSuite {
  def apply(spark: SparkSession, testConfiguration: TestConfiguration) = new DBLPSuite(spark, testConfiguration)
}

class DBLPSuite(spark: SparkSession, testConfiguration: TestConfiguration) extends TestSuite(spark, testConfiguration) {





  override def getName(): String = "DBLP"
  override def executeScenario(scenario: TestScenario): Unit = ???
}

