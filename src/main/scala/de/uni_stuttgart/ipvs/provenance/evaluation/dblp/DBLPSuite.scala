package de.uni_stuttgart.ipvs.provenance.evaluation.dblp


import de.uni_stuttgart.ipvs.provenance.evaluation.twitter.TwitterScenario1
import de.uni_stuttgart.ipvs.provenance.evaluation.{TestConfiguration, TestScenario, TestSuite}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

object DBLPSuite {
  def apply(spark: SparkSession, testConfiguration: TestConfiguration) = new DBLPSuite(spark, testConfiguration)
}

class DBLPSuite(spark: SparkSession, testConfiguration: TestConfiguration) extends TestSuite(spark, testConfiguration) {

  lazy override val  logger = LoggerFactory.getLogger(getClass)

  addScenario(new DBLPScenario2(spark, testConfiguration))
  addScenario(new DBLPScenario5i(spark, testConfiguration))
  addScenario(new DBLPScenario5p(spark, testConfiguration))
  addScenario(new DBLPScenario5wn(spark, testConfiguration))
  addScenario(new DBLPScenario1(spark, testConfiguration))

  override def getName(): String = "DBLP"
}

