package de.uni_stuttgart.ipvs.provenance.evaluation

import de.uni_stuttgart.ipvs.provenance.SharedSparkTestDataFrames
import de.uni_stuttgart.ipvs.provenance.evaluation.twitter.{TwitterScenario1, TwitterScenario2, TwitterScenario3, TwitterScenario4, TwitterScenario5}
import org.scalatest.FunSuite

class TwitterScenarios extends FunSuite with SharedSparkTestDataFrames {

  val pathToData = "src/main/external_resources/TwitterData/"
  val testConfiguration1 = TestConfiguration.local(pathToData)

  test("[Reference] Scenario 1"){
    val scenario = new TwitterScenario1(spark, testConfiguration1)
    scenario.referenceScenario.show(10)
  }

  test("[MSR] Scenario 1"){
    val scenario = new TwitterScenario1(spark, testConfiguration1)
    scenario.extendedScenario.show(10)
  }

  test("[Reference] Scenario 2"){
    val scenario = new TwitterScenario2(spark, testConfiguration1)
    scenario.referenceScenario.show(10)
  }

  test("[MSR] Scenario 2"){
    val scenario = new TwitterScenario2(spark, testConfiguration1)
    scenario.extendedScenario.show(10)
  }

  test("[Reference] Scenario 3"){
    val scenario = new TwitterScenario3(spark, testConfiguration1)
    scenario.referenceScenario.show(10)
  }

  test("[MSR] Scenario 3"){
    val scenario = new TwitterScenario3(spark, testConfiguration1)
    scenario.extendedScenario.show(10)
//    scenario.extendedScenario().explain(true)
  }

  test("[Reference] Scenario 4"){
    val scenario = new TwitterScenario4(spark, testConfiguration1)
    scenario.referenceScenario.show(10)
  }

  test("[MSR] Scenario 4"){
    val scenario = new TwitterScenario4(spark, testConfiguration1)
    scenario.extendedScenario.show(10)
  }

  test("[Reference] Scenario 5"){
    val scenario = new TwitterScenario5(spark, testConfiguration1)
    scenario.referenceScenario.show(10)
  }

  test("[MSR] Scenario 5"){
    val scenario = new TwitterScenario5(spark, testConfiguration1)
    scenario.extendedScenario.show(10)
  }

}
