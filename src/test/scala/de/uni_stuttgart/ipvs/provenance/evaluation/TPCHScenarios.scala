package de.uni_stuttgart.ipvs.provenance.evaluation

import de.uni_stuttgart.ipvs.provenance.SharedSparkTestInstance
import de.uni_stuttgart.ipvs.provenance.evaluation.tpch.TPCHScenario01
import de.uni_stuttgart.ipvs.provenance.evaluation.twitter.TwitterScenario1
import org.scalatest.FunSuite

class TPCHScenarios extends FunSuite with SharedSparkTestInstance {

  import spark.implicits._
  val pathToData = "src/main/external_resources/TPCH"
  val testConfiguration1 = TestConfiguration.local(pathToData, 1)

  //  SCENARIO 1
  test("[Reference] TPCH 01"){
    val scenario = new TPCHScenario01(spark, testConfiguration1)
    scenario.referenceScenario.show(10, false)
  }

}
