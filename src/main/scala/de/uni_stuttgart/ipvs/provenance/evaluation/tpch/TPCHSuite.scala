package de.uni_stuttgart.ipvs.provenance.evaluation.tpch

import de.uni_stuttgart.ipvs.provenance.evaluation.{TestConfiguration, TestSuite}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object TPCHSuite {
  def apply(spark: SparkSession, testConfiguration: TestConfiguration) = new TPCHSuite(spark, testConfiguration)
}

class TPCHSuite (spark: SparkSession, testConfiguration: TestConfiguration) extends TestSuite(spark, testConfiguration) {

  override def getName(): String = "TPC-H"
  lazy override val  logger = LoggerFactory.getLogger(getClass)

  addScenario(new TPCHScenario000(spark, testConfiguration))


  
}
