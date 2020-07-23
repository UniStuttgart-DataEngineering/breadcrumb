package de.uni_stuttgart.ipvs.provenance.evaluation

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

abstract class TestSuite(spark: SparkSession, testConfiguration: TestConfiguration) {

  lazy val logger = LoggerFactory.getLogger(getClass)
  lazy val pathToCache = "/user/hadoop/diesterf/cache/" + getName() + "/"
  lazy val resultWritePath = "/user/hadoop/diesterf/trash/" + getName() + "/"
  val scenarios = scala.collection.mutable.ListBuffer.empty[TestScenario]
  var selectedScenarios = selectScenarios()



  def getWritePath(scenarioName: String): String ={
    var path = resultWritePath
    path += scenarioName
    path
  }





  def getName() : String

  def addScenario(scenario: TestScenario) : Unit = {
    scenarios :+ scenario
  }

  def selectScenarios(): scala.collection.immutable.List[TestScenario] = {
    var testMask: Int = testConfiguration.testMask
    var selectedScenarios = scala.collection.mutable.ListBuffer.empty[TestScenario]
    for (scenario <- scenarios){
      if ((testMask & 1) == 1) {
        selectedScenarios += scenario
      }
      testMask >>= 1
    }
    selectedScenarios.toList
  }

  def executeScenarios(): Unit = {
    for (scenario <- selectedScenarios) {
      executeScenario(scenario)
    }
  }

  def clearCache() : Unit = {
    spark.sqlContext.clearCache()
  }

  def executeScenario(scenario: TestScenario): Unit = {
    for (iteration <- 0 until testConfiguration.iterations) {
      logger.warn(s"${scenario.getName} in iteration ${iteration} with data size ${testConfiguration.dataSize} begins")
      val t0 = System.nanoTime()
      val result = testConfiguration.referenceScenario match {
        case false => scenario.extendedScenario
        case _ => scenario.referenceScenario
      }
      collectDataFrame(result, scenario.getName)
      val t1 = System.nanoTime()
      logger.warn(s"${scenario.getName} in iteration ${iteration} with data size ${testConfiguration.dataSize}: ${(t1 - t0)} ns")
    }

  }

  def deleteResult(scenarioName: String) : Unit = {
    try{
      val hdfs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      val deletePaths = hdfs.globStatus(new Path(getWritePath(scenarioName: String) + "*") ).map(_.getPath)
      deletePaths.foreach{ path => hdfs.delete(path, true) }
    } catch {
      case e: Exception => {logger.warn("Deleting files went wrong", e.getStackTrace)}
    }
  }

  def collectDataFrame(df: Dataset[_], scenarioName: String): Unit = {
    df.write.mode(SaveMode.Overwrite).parquet(getWritePath(scenarioName))
    df.explain()
  }

}
