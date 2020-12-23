package de.uni_stuttgart.ipvs.provenance.evaluation

import de.uni_stuttgart.ipvs.provenance.SharedSparkTestInstance
import de.uni_stuttgart.ipvs.provenance.evaluation.tpch.{TPCHScenario00, TPCHScenario01, TPCHScenario03, TPCHScenario04, TPCHScenario12}
import de.uni_stuttgart.ipvs.provenance.nested_why_not.ProvenanceContext
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.scalatest.FunSuite

class TPCHScenarios extends FunSuite with SharedSparkTestInstance {

  import spark.implicits._
  val pathToData = "src/main/external_resources/TPCH"
  val testConfiguration1 = TestConfiguration.local(pathToData, 1)

  def collectDataFrameLocal(df: DataFrame, scenarioName: String): Unit = {
    df.write.mode(SaveMode.Overwrite).parquet(pathToData + scenarioName)
    df.explain()
  }


  //  SCENARIO 0 nestInputData
  test("[Reference] TPCH 00"){
    val scenario = new TPCHScenario00(spark, testConfiguration1)
    val res = scenario.referenceScenario

    //res.printSchema()
    //res.count()
    res.show(10, false)
  }

  //  SCENARIO 1
  test("[Reference] TPCH 01"){
    val scenario = new TPCHScenario01(spark, testConfiguration1)
    scenario.referenceScenario.show(10, false)
  }

  test("[RewriteWithSA] TPCH 01"){
    val scenario = new TPCHScenario01(spark, testConfiguration1)
    ProvenanceContext.setTestScenario(scenario)
    val res = scenario.extendedScenarioWithSA()
    res.explain()
    res.show(10,false)
    ProvenanceContext.setTestScenario(null)
  }

  //  SCENARIO 3
  test("[Reference] TPCH 03"){
    val scenario = new TPCHScenario03(spark, testConfiguration1)
    val res = scenario.referenceScenario
    res.show(10, false)
    res.explain()
  }

  test("[RewriteWithoutSA] TPCH 03"){
    val scenario = new TPCHScenario03(spark, testConfiguration1)
    val res = scenario.extendedScenarioWithoutSA
    res.show(10)
    res.explain()
  }

  test("[RewriteWithSAMSR] TPCH 03") {
    val scenario = new TPCHScenario03(spark, testConfiguration1)
    ProvenanceContext.setTestScenario(scenario)
    val res = scenario.extendedScenarioWithSAandMSR()
    res.show(10,false)
    res.explain()
    ProvenanceContext.setTestScenario(null)
  }


  //  SCENARIO 4
  test("[Reference] TPCH 04"){
    val scenario = new TPCHScenario04(spark, testConfiguration1)
    scenario.referenceScenario.show(10, false)
  }

  test("[RewriteWithoutSA] TPCH 04"){
    val scenario = new TPCHScenario04(spark, testConfiguration1)
    scenario.extendedScenarioWithoutSA.show(10)
  }

  test("[RewriteWithSAMSR] TPCH 04") {
    val scenario = new TPCHScenario04(spark, testConfiguration1)
    ProvenanceContext.setTestScenario(scenario)
    val res = scenario.extendedScenarioWithSAandMSR()
    res.show(10,false)
    res.explain()
    ProvenanceContext.setTestScenario(null)
  }

  test("[RewriteWithPreparedSAMSR] TPCH 04") {
    val scenario = new TPCHScenario04(spark, testConfiguration1)
    ProvenanceContext.setTestScenario(scenario)
    var res = scenario.prepareScenarioForMSRComputation()
    collectDataFrameLocal(res, scenario.getName + "intermediate")
    res = scenario.extendedScenarioWithPreparedSAandMSR()
    res.show(10)
    ProvenanceContext.setTestScenario(null)
  }


  //  SCENARIO 12
  test("[Reference] TPCH 12"){
    val scenario = new TPCHScenario12(spark, testConfiguration1)
    scenario.referenceScenario.show(10, false)
  }

}
