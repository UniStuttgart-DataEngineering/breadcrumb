package de.uni_stuttgart.ipvs.provenance.evaluation

import de.uni_stuttgart.ipvs.provenance.SharedSparkTestDataFrames
import de.uni_stuttgart.ipvs.provenance.evaluation.dblp.{DBLPScenario1, DBLPScenario2, DBLPScenario3, DBLPScenario4, DBLPScenario5}
import de.uni_stuttgart.ipvs.provenance.nested_why_not.ProvenanceContext
import org.scalatest.FunSuite
import org.apache.spark.sql.functions.explode

class DBLPScenarios extends FunSuite with SharedSparkTestDataFrames {

  val pathToData = "src/main/external_resources/DBLP/"
  val testConfiguration1 = TestConfiguration.local(pathToData)

  import spark.implicits._


  test("[Reference] Scenario 1"){
    val scenario = new DBLPScenario1(spark, testConfiguration1)
    scenario.referenceScenario.cache().show(10)
    scenario.referenceScenario.filter($"author".contains("Chapman")).show(20)
  }

  test("[MSR] Scenario 1"){
    val scenario = new DBLPScenario1(spark, testConfiguration1)
    scenario.extendedScenario.show(10)
    scenario.extendedScenario.filter($"author".contains("Chapman")).show(20)
  }


  test("[Reference] Scenario 2"){
    val scenario = new DBLPScenario2(spark, testConfiguration1)
    scenario.referenceScenario.show(10)
  }

  test("[MSR] Scenario 2"){
    val scenario = new DBLPScenario2(spark, testConfiguration1)
    //scenario.extendedScenario.show(10)
    ProvenanceContext.setTestScenario(scenario)
    //val toBeDebugged = scenario.extendedScenario.filter($"author".contains("Sudeepa Roy"))
    //toBeDebugged.explain()
    //toBeDebugged.show() //.withColumn("prov", explode($"__PROVENANCE_COLLECTION_0001")).show(50)
    ProvenanceContext.setTestScenario(null)
  }


  test("[Reference] Scenario 3"){
    val scenario = new DBLPScenario3(spark, testConfiguration1)
    scenario.referenceScenario.show(10)
  }

  test("[MSR] Scenario 3") {
    val scenario = new DBLPScenario3(spark, testConfiguration1)
    scenario.extendedScenario.show(10)
  }

  
  test("[Reference] Scenario 4"){
    val scenario = new DBLPScenario4(spark, testConfiguration1)
    scenario.referenceScenario.show(10)
  }

  test("[MSR] Scenario 4") {
    val scenario = new DBLPScenario4(spark, testConfiguration1)
    scenario.extendedScenario.show(10)
  }


  test("[Reference] Scenario 5"){
    val scenario = new DBLPScenario5(spark, testConfiguration1)
    scenario.referenceScenario.show(10)
  }

  test("[MSR] Scenario 5"){
    val scenario = new DBLPScenario5(spark, testConfiguration1)
    scenario.extendedScenario.show(10)
  }


}
