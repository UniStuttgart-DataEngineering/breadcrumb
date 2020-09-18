package de.uni_stuttgart.ipvs.provenance.evaluation

import de.uni_stuttgart.ipvs.provenance.SharedSparkTestDataFrames
import de.uni_stuttgart.ipvs.provenance.evaluation.crime._
import de.uni_stuttgart.ipvs.provenance.nested_why_not.ProvenanceContext
import org.scalatest.FunSuite


class CrimeScenarios extends FunSuite with SharedSparkTestDataFrames {

  import spark.implicits._
  val pathToData = "src/main/external_resources/Crime/"
  val testConfiguration1 = TestConfiguration.local(pathToData)


  // SCENARIO 1
  test("[Reference] Scenario 1"){
    val scenario = new CrimeScenario1(spark, testConfiguration1)
    var res = scenario.referenceScenario
    res = res.filter($"pname".contains("Roger") && $"type".contains("Laugh"))
    res.show(10)
    res
  }

  test("[RewriteWithoutSA] Scenario 1"){
    val scenario = new CrimeScenario1(spark, testConfiguration1)
    scenario.extendedScenarioWithoutSA.filter($"pname".contains("Roger") && $"type".contains("Laugh")).show(10)
  }


  // SCENARIO 2
  test("[Reference] Scenario 2"){
    val scenario = new CrimeScenario2(spark, testConfiguration1)
    var res = scenario.referenceScenario
    val constraint = "Conedera"
    res = res.filter($"pname".contains(constraint))
    res.show(10)
    res
  }

  test("[RewriteWithoutSA] Scenario 2"){
    val scenario = new CrimeScenario2(spark, testConfiguration1)
//    scenario.extendedScenarioWithoutSA.show(10)
    scenario.extendedScenarioWithoutSA.filter($"pname".contains("Conedera")).show(10)
  }

  test("[RewriteWithSA] Scenario 2"){
    val scenario = new CrimeScenario2(spark, testConfiguration1)
    ProvenanceContext.setTestScenario(scenario)
//    scenario.extendedScenarioWithSA
    val toBeDebugged = scenario.extendedScenarioWithSA.filter($"pname".contains("Conedera"))
//    toBeDebugged.explain()
    toBeDebugged.show() //.withColumn("prov", explode($"__PROVENANCE_COLLECTION_0001")).show(50)
    ProvenanceContext.setTestScenario(null)
  }

  test("[RewriteWithSAMSR] Scenario 2") {
    val scenario = new CrimeScenario2(spark, testConfiguration1)
    ProvenanceContext.setTestScenario(scenario)
    val res = scenario.extendedScenarioWithSAandMSR()
    res.show(10)
    ProvenanceContext.setTestScenario(null)
  }



}
