package de.uni_stuttgart.ipvs.provenance.evaluation

import de.uni_stuttgart.ipvs.provenance.SharedSparkTestDataFrames
import de.uni_stuttgart.ipvs.provenance.evaluation.dblp.{DBLPScenario1, DBLPScenario2, DBLPScenario3, DBLPScenario4, DBLPScenario5}
import org.scalatest.FunSuite
import org.apache.spark.sql.functions._


class DBLPScenarios extends FunSuite with SharedSparkTestDataFrames {

  import spark.implicits._
  val pathToData = "src/main/external_resources/DBLP/"
  val testConfiguration1 = TestConfiguration.local(pathToData)


  test("[Reference] Scenario 1"){
    val scenario = new DBLPScenario1(spark, testConfiguration1)
    scenario.referenceScenario.show(10)
//    scenario.referenceScenario.filter($"title".contains("Scalable algorithms for scholarly figure mining and semantics")).show(10)
  }

  test("[MSR] Scenario 1"){
    val scenario = new DBLPScenario1(spark, testConfiguration1)
    scenario.extendedScenario.show(10)
  }


  test("[Reference] Scenario 2"){
    val scenario = new DBLPScenario2(spark, testConfiguration1)
//    scenario.referenceScenario.show(10)
    scenario.referenceScenario.filter($"author".contains("Sudeepa Roy")).show(10)
  }

  test("[MSR] Scenario 2"){
    val scenario = new DBLPScenario2(spark, testConfiguration1)
    scenario.extendedScenario.show(10)
  }


  test("[Reference] Scenario 3"){
    val scenario = new DBLPScenario3(spark, testConfiguration1)
    scenario.referenceScenario.show(10)
//    scenario.referenceScenario.printSchema()
//    var res = scenario.referenceScenario.withColumn("fpairs", explode($"listOfAuthorPapers"))
////    res = res.filter($"fpairs.author".contains("Gail Corbitt") && $"fpairs.ititle".contains("Minitrack Introduction"))
//    res = res.withColumn("rauthor", explode($"fpairs.author"))
//    res = res.withColumn("title", $"fpairs.ititle")
////    res = res.filter( $"year" === 2006 && $"rauthor._VALUE".contains("Gail Corbitt"))
//    res = res.filter($"title".contains("Minitrack Introduction"))
//    res.show(10)
  }

  test("[MSR] Scenario 3") {
    val scenario = new DBLPScenario3(spark, testConfiguration1)
    scenario.extendedScenario.show(10)
  }

  
  test("[Reference] Scenario 4"){
    val scenario = new DBLPScenario4(spark, testConfiguration1)
    scenario.referenceScenario.show(10)
//    var res = scenario.referenceScenario
//    res = res.filter($"ipauthor".contains("George V. Tsoulos"))
//    res.show(10)
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
//    scenario.extendedScenario.explain(true)
  }


}
