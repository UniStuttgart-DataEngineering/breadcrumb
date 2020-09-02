package de.uni_stuttgart.ipvs.provenance.evaluation

import de.uni_stuttgart.ipvs.provenance.SharedSparkTestDataFrames
import de.uni_stuttgart.ipvs.provenance.evaluation.dblp.{DBLPScenario1, DBLPScenario2, DBLPScenario3, DBLPScenario3prime, DBLPScenario4, DBLPScenario5, DBLPScenario5prime}
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

//    val res = scenario.extendedScenario()
//    val res2 = res.filter($"title".contains("Why not?"))
//    val res3 = res2.filter($"author".contains("Adriane Chapman"))
//    res2.show()
//    res3.show()
  }


  test("[Reference] Scenario 3"){
    val scenario = new DBLPScenario3(spark, testConfiguration1)
    scenario.referenceScenario.show(10)
//    scenario.referenceScenario.filter($"author".contains("Sudeepa Roy")).show(false)
  }

  test("[MSR] Scenario 3") {
    val scenario = new DBLPScenario3(spark, testConfiguration1)
    scenario.extendedScenario.show(10)
  }


  test("[Reference] Scenario 3 prime"){
    val scenario = new DBLPScenario3prime(spark, testConfiguration1)
    scenario.referenceScenario.show(10)
//    scenario.referenceScenario.filter($"author".contains("Sudeepa Roy Dey")).show(false)
  }

  test("[MSR] Scenario 3 prime") {
    val scenario = new DBLPScenario3prime(spark, testConfiguration1)
    scenario.extendedScenario.show(10)
//    scenario.extendedScenario.filter($"author".contains("Sudeepa Roy Dey")).show(false)
  }


  test("[Reference] Scenario 4"){
    val scenario = new DBLPScenario4(spark, testConfiguration1)
    scenario.referenceScenario.show(10)
//    scenario.referenceScenario.filter($"title".contains("DBToaster: Agile Views for a Dynamic Data Management System.")).show(false)
  }

  test("[MSR] Scenario 4") {
    val scenario = new DBLPScenario4(spark, testConfiguration1)
    scenario.extendedScenario.show(10)
  }


  test("[Reference] Scenario 5"){
    val scenario = new DBLPScenario5(spark, testConfiguration1)
//    scenario.referenceScenario.show(10)
    scenario.referenceScenario.filter($"ipauthor".contains("Thomas Neumann")).show(false)
  }

  test("[MSR] Scenario 5"){
    val scenario = new DBLPScenario5(spark, testConfiguration1)
    scenario.extendedScenario.show(10)
//    scenario.extendedScenario.filter($"ipauthor".contains("Thomas Neumann")).show(false)
  }


  test("[Reference] Scenario 5 prime"){
    val scenario = new DBLPScenario5prime(spark, testConfiguration1)
    scenario.referenceScenario.show(10)
//    scenario.referenceScenario.filter($"allauthors".contains("Thomas Neumann")).show(false)
  }

  test("[MSR] Scenario 5 prime"){
    val scenario = new DBLPScenario5prime(spark, testConfiguration1)
    scenario.extendedScenario.show(false)
  }

}
