package de.uni_stuttgart.ipvs.provenance.evaluation

import de.uni_stuttgart.ipvs.provenance.nested_why_not.WhyNotProvenance
import de.uni_stuttgart.ipvs.provenance.why_not_question.Twig
import org.apache.spark.sql.{DataFrame, SparkSession}

abstract class TestScenario(spark: SparkSession, testConfiguration: TestConfiguration) {

  def getName: String

  def whyNotQuestion : Twig

  def referenceScenario : DataFrame

  def extendedScenario : DataFrame = {
    WhyNotProvenance.computeMSRs(referenceScenario, whyNotQuestion)
  }


}
