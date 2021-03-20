package de.uni_stuttgart.ipvs.provenance.evaluation

import de.uni_stuttgart.ipvs.provenance.why_not_question.Twig
import org.apache.spark.sql.{DataFrame, SparkSession}

object DummyScenario {

  var scenario : TestScenario = null

  def apply() = {
    if (scenario == null) {
      scenario = new DummyScenario(null, null)
    }
    scenario
  }
}

private class DummyScenario (spark: SparkSession, testConfiguration: TestConfiguration) extends TestScenario(spark, testConfiguration) {
  override def getName: String = "Dummy"

  override def whyNotQuestion: Twig = null

  override def referenceScenario: DataFrame = null
}
