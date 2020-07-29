package de.uni_stuttgart.ipvs.provenance.evaluation

import de.uni_stuttgart.ipvs.provenance.SharedSparkTestDataFrames
import de.uni_stuttgart.ipvs.provenance.evaluation.twitter.TwitterScenario1
import org.scalatest.FunSuite

class TwitterScenarios extends FunSuite with SharedSparkTestDataFrames {

  val pathToData = "src/main/external_resources/TwitterData/"
  val testConfiguration1 = TestConfiguration.default(pathToData)
  val testConfiguration2 = TestConfiguration.default(pathToData)

  test("[Reference] Scenario 1"){
    val scenario = new TwitterScenario1(spark, testConfiguration1)
    scenario.referenceScenario.show(10)
  }

  test("[MSR] Scenario 1"){
    val scenario = new TwitterScenario1(spark, testConfiguration1)
    scenario.extendedScenario.show(10)
  }



}
