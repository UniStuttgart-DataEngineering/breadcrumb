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

  // Generating nestedCustomer
//  addScenario(new TPCHScenario000(spark, testConfiguration))

  // Queries for flat
  addScenario(new TPCHScenario01(spark, testConfiguration))
  addScenario(new TPCHScenario03(spark, testConfiguration))
  addScenario(new TPCHScenario04(spark, testConfiguration))
  addScenario(new TPCHScenario06(spark, testConfiguration))
  addScenario(new TPCHScenario10(spark, testConfiguration))
  addScenario(new TPCHScenario13(spark, testConfiguration))

  // Queries for nesting
//  addScenario(new TPCHScenario101(spark, testConfiguration))
//  addScenario(new TPCHScenario103(spark, testConfiguration))
//  addScenario(new TPCHScenario104(spark, testConfiguration))
//  addScenario(new TPCHScenario106(spark, testConfiguration))
//  addScenario(new TPCHScenario110(spark, testConfiguration))
//  addScenario(new TPCHScenario113(spark, testConfiguration))
}
