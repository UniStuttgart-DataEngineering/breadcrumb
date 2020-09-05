package de.uni_stuttgart.ipvs.provenance.evaluation

import de.uni_stuttgart.ipvs.provenance.SharedSparkTestDataFrames
import de.uni_stuttgart.ipvs.provenance.evaluation.dblp.{DBLPScenario1, DBLPScenario2, DBLPScenario6, DBLPScenario7, DBLPScenario4, DBLPScenario8, DBLPScenario3, DBLPScenario5}
import de.uni_stuttgart.ipvs.provenance.evaluation.twitter.TwitterScenario1
import org.scalatest.FunSuite

class DBLPScenarios extends FunSuite with SharedSparkTestDataFrames {

  import spark.implicits._
  val pathToData = "src/main/external_resources/DBLP/"
  val testConfiguration1 = TestConfiguration.local(pathToData)


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
