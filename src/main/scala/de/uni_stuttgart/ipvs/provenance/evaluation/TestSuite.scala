package de.uni_stuttgart.ipvs.provenance.evaluation

import de.uni_stuttgart.ipvs.provenance.nested_why_not.ProvenanceContext
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

abstract class TestSuite(spark: SparkSession, testConfiguration: TestConfiguration) {

  lazy val logger = LoggerFactory.getLogger(getClass)
  lazy val resultWritePath = getBasePath() + "results/" + getName() + "/"
  val scenarios = scala.collection.mutable.ListBuffer.empty[TestScenario]
  lazy val selectedScenarios = selectScenarios()

  val evaluationResult = EvaluationResult(spark, this)



  def getWritePath(scenarioName: String): String ={
    var path = resultWritePath
    path += scenarioName
    path
  }





  def getName() : String

  def addScenario(scenario: TestScenario) : Unit = {
    scenarios.append(scenario)
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
    evaluationResult.writeRunHeaderRow()
    evaluationResult.writeRunsHeaderRow()
    logger.warn("ScenariosNumber: " + selectedScenarios.size.toString())
    for (scenario <- selectedScenarios) {
      logger.warn("Execute Scenario: " + scenario.getName())
      executeScenario(scenario)
//      deleteResult(scenario.getName)
    }
  }

  def clearCache() : Unit = {
    spark.sqlContext.clearCache()
  }

  def executeScenarioIteration(scenario: TestScenario, iteration: Int): DataFrame = {
    val result = testConfiguration.referenceScenario match {
      case 1 => scenario.extendedScenario()
      case 2 => scenario.extendedScenarioWithoutSA()
      case 3 => scenario.extendedScenarioWithSA()
      case 4 => scenario.extendedScenarioWithSAandMSR()
      case 5 => scenario.extendedScenarioWithPreparedSAandMSR()
      case _ => scenario.referenceScenario()
    }
    logger.warn(s"${scenario.getName} in iteration ${iteration} with data size ${testConfiguration.dataSize} begins")
    val t0 = System.nanoTime()
    collectDataFrame(result, scenario.getName)
    val t1 = System.nanoTime()
    logger.warn(s"${scenario.getName} in iteration ${iteration} with data size ${testConfiguration.dataSize}: ${(t1 - t0)} ns")
    if (iteration >= 0) {
      evaluationResult.recordEntry(scenario, iteration, t1-t0)
    }

//    evaluationResult.getExpl(scenario, iteration, result)
    result
  }

  def executeScenario(scenario: TestScenario): Unit = {
    ProvenanceContext.setTestScenario(scenario)
    var result = spark.emptyDataFrame
    evaluationResult.reset()
    if (testConfiguration.referenceScenario == 5) {
      val res = scenario.prepareScenarioForMSRComputation()
      collectDataFrame(res, scenario.getName + "intermediate")
    }
    if (testConfiguration.warmUp) {
      result = executeScenarioIteration(scenario, -1)
    }
    for (iteration <- 0 until testConfiguration.iterations) {
      result = executeScenarioIteration(scenario, iteration)
    }
    evaluationResult.writeRunsRow(scenario)
    logger.debug("Analyzed Plan: ")
    logger.debug(result.queryExecution.analyzed.toString())
    logger.debug("Executed Plan: ")
    logger.debug(result.queryExecution.executedPlan.toString())
    ProvenanceContext.setTestScenario(null)

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

//  def collectDataFrame(df: Dataset[_], scenarioName: String): Unit = {
  def collectDataFrame(df: DataFrame, scenarioName: String): Unit = {
    df.write.mode(SaveMode.Overwrite).parquet(getWritePath(scenarioName))
//    df.write.mode(SaveMode.Overwrite).json(getWritePath(scenarioName))
    df.explain()
  }

  def toCSVHeader(): String = {
    val builder = scala.collection.mutable.StringBuilder.newBuilder
    builder.append("TestSuite")
    builder.append(";")
    builder.append(testConfiguration.toCSVHeader())
    builder.toString()

  }

  def toCSV(): String = {
    val builder = scala.collection.mutable.StringBuilder.newBuilder
    builder.append(getName())
    builder.append(";")
    builder.append(testConfiguration.toCSV())
    builder.toString()
  }

  def getBasePath(): String = {
    val parts = testConfiguration.pathToData.split("data")
    parts(0)
  }

}
