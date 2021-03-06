package de.uni_stuttgart.ipvs.provenance.evaluation

import de.uni_stuttgart.ipvs.provenance.nested_why_not.WhyNotProvenance
import de.uni_stuttgart.ipvs.provenance.why_not_question.Twig
import org.apache.spark.sql.{DataFrame, SparkSession}

abstract class TestScenario(spark: SparkSession, testConfiguration: TestConfiguration) {

  def getName(): String

  def whyNotQuestion(): Twig

  def referenceScenario() : DataFrame

  def extendedScenario() : DataFrame = {
    WhyNotProvenance.computeMSRs(referenceScenario, whyNotQuestion)
  }

  def toCSV(iteration: Int, exeutionTime: Long): String = {
    val builder = scala.collection.mutable.StringBuilder.newBuilder
    builder.append(toCSV())
    builder.append(iteration)
    builder.append(";")
    builder.append(exeutionTime)
    builder.append(";")
    builder.toString()
  }

  def toCSV(): String = {
    val builder = scala.collection.mutable.StringBuilder.newBuilder
    builder.append(getName)
    builder.append(";")
    builder.toString()
  }

  def toCSVHeader(iteration: Int, exeutionTime: Long ): String = {
    val builder = scala.collection.mutable.StringBuilder.newBuilder
    builder.append(toCSVHeader())
    builder.append("Iteration")
    builder.append(";")
    builder.append("Time (ns)")
    builder.append(";")
    builder.toString()
  }

  def toCSVHeader(): String = {
    val builder = scala.collection.mutable.StringBuilder.newBuilder
    builder.append("ScenarioName")
    builder.append(";")
    builder.toString()
  }


}
