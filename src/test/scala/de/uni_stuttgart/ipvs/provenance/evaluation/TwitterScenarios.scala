package de.uni_stuttgart.ipvs.provenance.evaluation

import de.uni_stuttgart.ipvs.provenance.SharedSparkTestDataFrames
import de.uni_stuttgart.ipvs.provenance.evaluation.twitter.{TwitterScenario1, TwitterScenario2, TwitterScenario3, TwitterScenario4, TwitterScenario5, TwitterScenario6}
import de.uni_stuttgart.ipvs.provenance.nested_why_not.ProvenanceContext
import org.apache.spark.sql.functions.explode
import org.scalatest.FunSuite

class TwitterScenarios extends FunSuite with SharedSparkTestDataFrames {

  import spark.implicits._
  val pathToData = "src/main/external_resources/TwitterData/"
  val testConfiguration1 = TestConfiguration.local(pathToData)


//  SCENARIO 1
  test("[Reference] Scenario 1"){
    val scenario = new TwitterScenario1(spark, testConfiguration1)
    scenario.referenceScenario.show(10, false)
  }

  test("[RewriteWithoutSA] Scenario 1"){
    val scenario = new TwitterScenario1(spark, testConfiguration1)
    scenario.extendedScenarioWithoutSA.show(10)
  }

  test("[RewriteWithSA] Scenario 1"){
    val scenario = new TwitterScenario1(spark, testConfiguration1)
//    scenario.extendedScenarioWithSA.show(10)
    ProvenanceContext.setTestScenario(scenario)
//    scenario.extendedScenarioWithSA
    val toBeDebugged = scenario.extendedScenarioWithSA.filter($"id_str".contains("1027612080084414464"))
    toBeDebugged.explain()
    toBeDebugged.show() //.withColumn("prov", explode($"__PROVENANCE_COLLECTION_0001")).show(50)
    ProvenanceContext.setTestScenario(null)
  }

  test("[MSR] Scenario 1"){
    val scenario = new TwitterScenario1(spark, testConfiguration1)
    scenario.extendedScenario.show(10)
  }

  test("[RewriteWithSAMSR] Scenario 1") {
    val scenario = new TwitterScenario1(spark, testConfiguration1)
    ProvenanceContext.setTestScenario(scenario)
    val res = scenario.extendedScenarioWithSAandMSR()
    res.show(false)
    res.explain()
    ProvenanceContext.setTestScenario(null)
  }


//  SCENARIO 2
  test("[Reference] Scenario 2"){
    val scenario = new TwitterScenario2(spark, testConfiguration1)
    scenario.referenceScenario.show(10)
  }

  test("[RewriteWithoutSA] Scenario 2"){
    val scenario = new TwitterScenario2(spark, testConfiguration1)
    scenario.extendedScenarioWithoutSA.show(10)
  }

  test("[RewriteWithSA] Scenario 2") {
    val scenario = new TwitterScenario2(spark, testConfiguration1)
//    scenario.extendedScenarioWithSA.show(10)
    ProvenanceContext.setTestScenario(scenario)
//    scenario.extendedScenarioWithSA
    var toBeDebugged = scenario.extendedScenarioWithSA
//    toBeDebugged = toBeDebugged.withColumn("lname", explode($"listOfNames")).filter($"lname".contains("Cindy"))
    toBeDebugged.explain()
    toBeDebugged.show() //.withColumn("prov", explode($"__PROVENANCE_COLLECTION_0001")).show(50)
    ProvenanceContext.setTestScenario(null)
  }

  test("[MSR] Scenario 2"){
    val scenario = new TwitterScenario2(spark, testConfiguration1)
    scenario.extendedScenario.show(10)
  }

  test("[RewriteWithSAMSR] Scenario 2") {
    val scenario = new TwitterScenario2(spark, testConfiguration1)
    ProvenanceContext.setTestScenario(scenario)
    val res = scenario.extendedScenarioWithSAandMSR()
    res.show(false)
    res.explain()
    ProvenanceContext.setTestScenario(null)
  }


// SCENARIO 3
//  test("[Reference] Scenario 3"){
//    val scenario = new TwitterScenario3(spark, testConfiguration1)
//    scenario.referenceScenario //.filter($"screen_name".contains("YouTube")).show(50, false)
//  }
//
//  test("[RewriteWithoutSA] Scenario 3"){
//    val scenario = new TwitterScenario3(spark, testConfiguration1)
//    scenario.extendedScenarioWithoutSA //.filter($"screen_name".contains("YouTube")).show(10)
//  }
//
//  test("[RewriteWithSA] Scenario 3") {
//    val scenario = new TwitterScenario3(spark, testConfiguration1)
//    //    scenario.extendedScenarioWithSA.show(10)
//    ProvenanceContext.setTestScenario(scenario)
//    //    scenario.extendedScenarioWithSA
//    val toBeDebugged = scenario.extendedScenarioWithSA.filter($"screen_name".contains("YouTube"))
//    toBeDebugged.explain()
//    toBeDebugged.show() //.withColumn("prov", explode($"__PROVENANCE_COLLECTION_0001")).show(50)
//    ProvenanceContext.setTestScenario(null)
//  }
//
//  test("[MSR] Scenario 3"){
//    val scenario = new TwitterScenario3(spark, testConfiguration1)
//    scenario.extendedScenario.show(10)
//    //    scenario.extendedScenario().explain(true)
//  }


  test("[Reference] Scenario 3"){
    val scenario = new TwitterScenario6(spark, testConfiguration1)
    scenario.referenceScenario.show(50, false) //.filter($"name".contains("Vanessa Tuqueque")).show(50, false)
  }

  test("[RewriteWithoutSA] Scenario 3"){
    val scenario = new TwitterScenario6(spark, testConfiguration1)
    scenario.extendedScenarioWithoutSA.filter($"name".contains("Vanessa Tuqueque")).show(50, false)
  }

  test("[RewriteWithSA] Scenario 3") {
    val scenario = new TwitterScenario6(spark, testConfiguration1)
    ProvenanceContext.setTestScenario(scenario)
//    scenario.extendedScenarioWithSA.explain(true)
    scenario.extendedScenarioWithSA.filter($"name".contains("Coca cola")).show(20)
    ProvenanceContext.setTestScenario(null)
  }

  test("[MSR] Scenario 3"){
    val scenario = new TwitterScenario6(spark, testConfiguration1)
    scenario.extendedScenario.show(10)
    //    scenario.extendedScenario().explain(true)
  }

  test("[RewriteWithSAMSR] Scenario 3") {
    val scenario = new TwitterScenario6(spark, testConfiguration1)
    ProvenanceContext.setTestScenario(scenario)
    val res = scenario.extendedScenarioWithSAandMSR()
    res.show(false)
    res.explain()
    ProvenanceContext.setTestScenario(null)
  }




  // SCENARIO 4
  test("[Reference] Scenario 4"){
    val scenario = new TwitterScenario4(spark, testConfiguration1)
    var res = scenario.referenceScenario
//    res = res.filter($"hashtagText".contains("Warcraft"))
    res.show(20, false)
  }

  test("[RewriteWithoutSA] Scenario 4"){
    val scenario = new TwitterScenario4(spark, testConfiguration1)
    scenario.extendedScenarioWithoutSA.show(10)
  }

  test("[RewriteWithSA] Scenario 4") {
    val scenario = new TwitterScenario4(spark, testConfiguration1)
    //    scenario.extendedScenarioWithSA.show(10)
    ProvenanceContext.setTestScenario(scenario)
//    scenario.extendedScenarioWithSA
    var toBeDebugged = scenario.extendedScenarioWithSA.filter($"hashtagText".contains("Arsenal"))
    toBeDebugged.explain()
    toBeDebugged.show() //.withColumn("prov", explode($"__PROVENANCE_COLLECTION_0001")).show(50)
    ProvenanceContext.setTestScenario(null)
  }

  test("[MSR] Scenario 4"){
    val scenario = new TwitterScenario4(spark, testConfiguration1)
    scenario.extendedScenario.show(10)
  }

  test("[RewriteWithSAMSR] Scenario 4") {
    val scenario = new TwitterScenario4(spark, testConfiguration1)
    ProvenanceContext.setTestScenario(scenario)
    val res = scenario.extendedScenarioWithSAandMSR()
    res.show(false)
    res.explain()
    ProvenanceContext.setTestScenario(null)
  }


  // SCENARIO 5
  test("[Reference] Scenario 5"){
    val scenario = new TwitterScenario5(spark, testConfiguration1)
    scenario.referenceScenario.show(50, false)
  }

  test("[RewriteWithoutSA] Scenario 5"){
    val scenario = new TwitterScenario5(spark, testConfiguration1)
    scenario.extendedScenarioWithoutSA.show(10)
  }

  test("[RewriteWithSA] Scenario 5") {
    val scenario = new TwitterScenario5(spark, testConfiguration1)
    //    scenario.extendedScenarioWithSA.show(10)
    ProvenanceContext.setTestScenario(scenario)
//    scenario.extendedScenarioWithSA
    val toBeDebugged = scenario.extendedScenarioWithSA.filter($"pname".contains("San Diego"))
    toBeDebugged.explain()
    toBeDebugged.show(false) //.withColumn("prov", explode($"__PROVENANCE_COLLECTION_0001")).show(50)
    ProvenanceContext.setTestScenario(null)
  }

  test("[MSR] Scenario 5"){
    val scenario = new TwitterScenario5(spark, testConfiguration1)
    scenario.extendedScenario.show(10)
  }

  test("[RewriteWithSAMSR] Scenario 5") {
    val scenario = new TwitterScenario5(spark, testConfiguration1)
    ProvenanceContext.setTestScenario(scenario)
    val res = scenario.extendedScenarioWithSAandMSR()
    res.show(false)
//    res.explain()
    ProvenanceContext.setTestScenario(null)
  }



//  test("[Reference] Scenario 6"){
//    val scenario = new TwitterScenario6(spark, testConfiguration1)
//    scenario.referenceScenario.filter($"name".contains("Vanessa Tuqueque")).show(50, false)
//  }
//
//  test("[Reference] Scenario 6a"){
//    val scenario = new TwitterScenario6(spark, testConfiguration1)
//    scenario.referenceScenario.filter($"name".contains("American Express")).show(50, false)
//  }
//
//  test("[RewriteWithSA] Scenario 6") {
//    val scenario = new TwitterScenario6(spark, testConfiguration1)
//    ProvenanceContext.setTestScenario(scenario)
//    scenario.extendedScenarioWithSA.filter($"name".contains("Vanessa Tuqueque")).show(20)
//    ProvenanceContext.setTestScenario(null)
//  }

}
