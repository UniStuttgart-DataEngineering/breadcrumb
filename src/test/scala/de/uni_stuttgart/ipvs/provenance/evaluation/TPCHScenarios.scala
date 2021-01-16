package de.uni_stuttgart.ipvs.provenance.evaluation

import de.uni_stuttgart.ipvs.provenance.SharedSparkTestInstance
import de.uni_stuttgart.ipvs.provenance.evaluation.tpch.{TPCHScenario00, TPCHScenario000, TPCHScenario001, TPCHScenario01, TPCHScenario02, TPCHScenario03, TPCHScenario04, TPCHScenario05, TPCHScenario06, TPCHScenario07, TPCHScenario10, TPCHScenario12}
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


  //  SCENARIO 0 nestOrders
  test("[Reference] TPCH 00"){
    val scenario = new TPCHScenario00(spark, testConfiguration1)
    val res = scenario.referenceScenario

    //res.printSchema()
    //res.count()
    res.show(100, false)
  }

  //  SCENARIO 000 nestCustomers
  test("[Reference] TPCH 000"){
    val scenario = new TPCHScenario000(spark, testConfiguration1)
    val res = scenario.referenceScenario

//    res.printSchema()
    println(res.count())
    res.show(10, false)
  }

  //  SCENARIO 001 sampleInputData
  test("[Reference] TPCH 001"){
    val scenario = new TPCHScenario001(spark, testConfiguration1)
    val res = scenario.referenceScenario

    //res.printSchema()
    //res.count()
    res.show(10, false)
  }


  //  SCENARIO 1
  test("[Reference] TPCH 01"){
    val scenario = new TPCHScenario01(spark, testConfiguration1)
    val res = scenario.referenceScenario
    res.explain()
    res.show(10, false)
  }

  test("[RewriteWithSA] TPCH 01"){
    val scenario = new TPCHScenario01(spark, testConfiguration1)
    ProvenanceContext.setTestScenario(scenario)
    val res = scenario.extendedScenarioWithSA()
    res.explain()
    res.show(10,false)
    ProvenanceContext.setTestScenario(null)
  }

  test("[RewriteWithoutSA] TPCH 01"){
    val scenario = new TPCHScenario01(spark, testConfiguration1)
    scenario.extendedScenarioWithoutSA.show(10)
  }

  test("[RewriteWithSAMSR] TPCH 01") {
    val scenario = new TPCHScenario01(spark, testConfiguration1)
    ProvenanceContext.setTestScenario(scenario)
    val res = scenario.extendedScenarioWithSAandMSR()
    res.show(10,false)
    res.explain()
    ProvenanceContext.setTestScenario(null)
  }

  test("[RewriteWithPreparedSAMSR] TPCH 01") {
    val scenario = new TPCHScenario01(spark, testConfiguration1)
    ProvenanceContext.setTestScenario(scenario)
    var res = scenario.prepareScenarioForMSRComputation()
    collectDataFrameLocal(res, scenario.getName + "intermediate")
    res = scenario.extendedScenarioWithPreparedSAandMSR()
    res.show(10)
    ProvenanceContext.setTestScenario(null)
  }


  //  SCENARIO 2
  test("[Reference] TPCH 02"){
    val scenario = new TPCHScenario02(spark, testConfiguration1)
    val res = scenario.referenceScenario
    //res.explain()
    //val plan = res.queryExecution.analyzed
    //println(plan)
    res.printSchema()
    res.show(10, false)
  }

  test("[RewriteWithSA] TPCH 02"){
    val scenario = new TPCHScenario02(spark, testConfiguration1)
    ProvenanceContext.setTestScenario(scenario)
    val res = scenario.extendedScenarioWithSA
    res.show(10)
    res.explain()
    ProvenanceContext.setTestScenario(null)
  }

  test("[RewriteWithSAMSAR] TPCH 02"){
    val scenario = new TPCHScenario02(spark, testConfiguration1)
    ProvenanceContext.setTestScenario(scenario)
    val res = scenario.extendedScenarioWithSAandMSR()
    res.show(10)
    res.explain()
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

  test("[RewriteWithSA] TPCH 03"){
    val scenario = new TPCHScenario03(spark, testConfiguration1)
    ProvenanceContext.setTestScenario(scenario)
    var res = scenario.extendedScenarioWithSA()
    res = res.filter($"l_orderkey" === 4016674)
    res.show(10)
    res.explain(true)
    ProvenanceContext.setTestScenario(null)
  }

  test("[RewriteWithSAMSR] TPCH 03") {
    val scenario = new TPCHScenario03(spark, testConfiguration1)
    ProvenanceContext.setTestScenario(scenario)
    var res = scenario.extendedScenarioWithSAandMSR()
    res.show(50,false)
    res.explain()
    ProvenanceContext.setTestScenario(null)
  }

  test("[RewriteWithPreparedSAMSR] TPCH 03") {
    val scenario = new TPCHScenario03(spark, testConfiguration1)
    ProvenanceContext.setTestScenario(scenario)
    var res = scenario.prepareScenarioForMSRComputation()
    collectDataFrameLocal(res, scenario.getName + "intermediate")
    res = scenario.extendedScenarioWithPreparedSAandMSR()
    res.show(10)
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

  test("[RewriteWithSA] TPCH 04"){
    val scenario = new TPCHScenario04(spark, testConfiguration1)
    ProvenanceContext.setTestScenario(scenario)
    val res = scenario.extendedScenarioWithSA()
    res.show(10)
    res.explain(true)
    ProvenanceContext.setTestScenario(null)
  }

  test("[RewriteWithSAMSR] TPCH 04") {
    val scenario = new TPCHScenario04(spark, testConfiguration1)
    ProvenanceContext.setTestScenario(scenario)
    val res = scenario.extendedScenarioWithSAandMSR()
    res.show(10,false)
    res.explain(true)
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



  //  SCENARIO 5
  test("[Reference] TPCH 05"){
    val scenario = new TPCHScenario05(spark, testConfiguration1)
    scenario.referenceScenario.show(10, false)
  }

  test("[RewriteWithoutSA] TPCH 05"){
    val scenario = new TPCHScenario05(spark, testConfiguration1)
    scenario.extendedScenarioWithoutSA.show(10)
  }

  test("[RewriteWithSAMSR] TPCH 05") {
    val scenario = new TPCHScenario05(spark, testConfiguration1)
    ProvenanceContext.setTestScenario(scenario)
    val res = scenario.extendedScenarioWithSAandMSR()
    res.show(10,false)
    res.explain()
    ProvenanceContext.setTestScenario(null)
  }

  test("[RewriteWithPreparedSAMSR] TPCH 05") {
    val scenario = new TPCHScenario05(spark, testConfiguration1)
    ProvenanceContext.setTestScenario(scenario)
    var res = scenario.prepareScenarioForMSRComputation()
    collectDataFrameLocal(res, scenario.getName + "intermediate")
    res = scenario.extendedScenarioWithPreparedSAandMSR()
    res.show(10)
    ProvenanceContext.setTestScenario(null)
  }



  //  SCENARIO 6
  test("[Reference] TPCH 06"){
    val scenario = new TPCHScenario06(spark, testConfiguration1)
    scenario.referenceScenario.show(10, false)
  }

  test("[RewriteWithoutSA] TPCH 06"){
    val scenario = new TPCHScenario06(spark, testConfiguration1)
    scenario.extendedScenarioWithoutSA.show(10)
  }

  test("[RewriteWithSA] TPCH 06"){
    val scenario = new TPCHScenario06(spark, testConfiguration1)
    ProvenanceContext.setTestScenario(scenario)
    val res = scenario.extendedScenarioWithSA()
    res.show(10)
    res.explain()
    ProvenanceContext.setTestScenario(null)
  }

  test("[RewriteWithSAMSR] TPCH 06") {
    val scenario = new TPCHScenario06(spark, testConfiguration1)
    ProvenanceContext.setTestScenario(scenario)
    val res = scenario.extendedScenarioWithSAandMSR()
    res.show(10,false)
    res.explain()
    ProvenanceContext.setTestScenario(null)
  }

  test("[RewriteWithPreparedSAMSR] TPCH 06") {
    val scenario = new TPCHScenario06(spark, testConfiguration1)
    ProvenanceContext.setTestScenario(scenario)
    var res = scenario.prepareScenarioForMSRComputation()
    collectDataFrameLocal(res, scenario.getName + "intermediate")
    res = scenario.extendedScenarioWithPreparedSAandMSR()
    res.show(10)
    ProvenanceContext.setTestScenario(null)
  }



  //  SCENARIO 7
  test("[Reference] TPCH 07"){
    val scenario = new TPCHScenario07(spark, testConfiguration1)
    scenario.referenceScenario.show(10, false)
  }

  test("[RewriteWithoutSA] TPCH 07"){
    val scenario = new TPCHScenario07(spark, testConfiguration1)
    scenario.extendedScenarioWithoutSA.show(10)
  }

  test("[RewriteWithSAMSR] TPCH 07") {
    val scenario = new TPCHScenario07(spark, testConfiguration1)
    ProvenanceContext.setTestScenario(scenario)
    val res = scenario.extendedScenarioWithSAandMSR()
    res.show(10,false)
    res.explain()
    ProvenanceContext.setTestScenario(null)
  }

  test("[RewriteWithPreparedSAMSR] TPCH 07") {
    val scenario = new TPCHScenario07(spark, testConfiguration1)
    ProvenanceContext.setTestScenario(scenario)
    var res = scenario.prepareScenarioForMSRComputation()
    collectDataFrameLocal(res, scenario.getName + "intermediate")
    res = scenario.extendedScenarioWithPreparedSAandMSR()
    res.show(10)
    ProvenanceContext.setTestScenario(null)
  }



  //  SCENARIO 10
  test("[Reference] TPCH 10"){
    val scenario = new TPCHScenario10(spark, testConfiguration1)
    scenario.referenceScenario.show(10, false)
  }

  test("[RewriteWithoutSA] TPCH 10"){
    val scenario = new TPCHScenario10(spark, testConfiguration1)
    scenario.extendedScenarioWithoutSA.show(10)
  }

  test("[RewriteWithSAMSR] TPCH 10") {
    val scenario = new TPCHScenario10(spark, testConfiguration1)
    ProvenanceContext.setTestScenario(scenario)
    val res = scenario.extendedScenarioWithSAandMSR()
    res.show(10,false)
    res.explain()
    ProvenanceContext.setTestScenario(null)
  }

  test("[RewriteWithPreparedSAMSR] TPCH 10") {
    val scenario = new TPCHScenario10(spark, testConfiguration1)
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

  test("[RewriteWithoutSA] TPCH 12"){
    val scenario = new TPCHScenario12(spark, testConfiguration1)
    scenario.extendedScenarioWithoutSA.show(10)
  }

  test("[RewriteWithSAMSR] TPCH 12") {
    val scenario = new TPCHScenario12(spark, testConfiguration1)
    ProvenanceContext.setTestScenario(scenario)
    val res = scenario.extendedScenarioWithSAandMSR()
    res.show(10,false)
    res.explain()
    ProvenanceContext.setTestScenario(null)
  }

  test("[RewriteWithPreparedSAMSR] TPCH 12") {
    val scenario = new TPCHScenario12(spark, testConfiguration1)
    ProvenanceContext.setTestScenario(scenario)
    var res = scenario.prepareScenarioForMSRComputation()
    collectDataFrameLocal(res, scenario.getName + "intermediate")
    res = scenario.extendedScenarioWithPreparedSAandMSR()
    res.show(10)
    ProvenanceContext.setTestScenario(null)
  }

}