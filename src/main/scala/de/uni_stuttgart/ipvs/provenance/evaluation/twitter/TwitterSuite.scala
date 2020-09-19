package de.uni_stuttgart.ipvs.provenance.evaluation.twitter

import de.uni_stuttgart.ipvs.provenance.evaluation.{TestConfiguration, TestSuite}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

object TwitterSuite {
  def apply(spark: SparkSession, testConfiguration: TestConfiguration) = new TwitterSuite(spark, testConfiguration)
}

class TwitterSuite(spark: SparkSession, testConfiguration: TestConfiguration) extends TestSuite(spark, testConfiguration) {

  lazy override val  logger = LoggerFactory.getLogger(getClass)

  addScenario(new TwitterScenario1(spark, testConfiguration))
  addScenario(new TwitterScenario2(spark, testConfiguration))
//  addScenario(new TwitterScenario3(spark, testConfiguration))
  addScenario(new TwitterScenario6(spark, testConfiguration)) // T6 replaces T3
//  addScenario(new TwitterScenario4(spark, testConfiguration))
  addScenario(new TwitterScenario5(spark, testConfiguration))

  override def getName(): String = "Twitter"

}

//bin/hadoop fs -get /user/hadoop/diesterf/data/twitter/logs/08_09_17_00_stream000000.log ~/08_09_17_00_stream000000.log
//scp hadoop@bigmaster:/home/hadoop/08_09_17_00_stream000000.log ~/Documents/nested-why-not-spark/src/main/resources/TwitterData/08_09_17_00_stream000000.log
