package de.uni_stuttgart.ipvs.provenance.evaluation

import de.uni_stuttgart.ipvs.provenance.SharedSparkTestDataFrames
import de.uni_stuttgart.ipvs.provenance.evaluation.dblp.{DBLPScenario1, DBLPScenario2}
import de.uni_stuttgart.ipvs.provenance.evaluation.twitter.TwitterScenario1
import org.scalatest.FunSuite

class DBLPScenarios extends FunSuite with SharedSparkTestDataFrames {

  val pathToData = "src/main/external_resources/DBLP/"
  val testConfiguration1 = TestConfiguration.default(pathToData)

  test("[Reference] Scenario 1"){
    val scenario = new DBLPScenario1(spark, testConfiguration1)
    scenario.referenceScenario.show(10)
  }

  test("[MSR] Scenario 1"){
    val scenario = new DBLPScenario1(spark, testConfiguration1)
    scenario.extendedScenario.show(10)
  }

  test("[Reference] Scenario 2"){
    val scenario = new DBLPScenario2(spark, testConfiguration1)
    scenario.referenceScenario.show(10)
  }

  test("[MSR] Scenario 2"){
    val scenario = new DBLPScenario2(spark, testConfiguration1)
    scenario.extendedScenario.show(10)
  }

}
