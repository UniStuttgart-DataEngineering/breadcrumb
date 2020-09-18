package de.uni_stuttgart.ipvs.provenance.evaluation

import de.uni_stuttgart.ipvs.provenance.nested_why_not.{ProvenanceAttribute, ProvenanceContext, WhyNotMSRComputation, WhyNotProvenance}
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.{PrimarySchemaSubsetTree, SchemaSubsetTree}
import de.uni_stuttgart.ipvs.provenance.why_not_question.Twig
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.{DataFrame, SparkSession}

abstract class TestScenario(spark: SparkSession, testConfiguration: TestConfiguration) {

  def getName(): String

  def whyNotQuestion(): Twig

  def referenceScenario() : DataFrame

  var currentDataFrame: DataFrame = null

  def extendedScenarioWithoutSA() : DataFrame = {
    WhyNotProvenance.rewrite(referenceScenario, whyNotQuestion)
  }

  def extendedScenarioWithSA() : DataFrame = {
    WhyNotProvenance.rewriteWithAlternatives(referenceScenario, whyNotQuestion)
  }

  def extendedScenario() : DataFrame = {
    WhyNotProvenance.computeMSRs(referenceScenario, whyNotQuestion)
  }

  def extendedScenarioWithSAandMSR() : DataFrame = {
    WhyNotProvenance.computeMSRsWithAlternatives(referenceScenario, whyNotQuestion)
  }

  def prepareScenarioForMSRComputation(): DataFrame = {
    currentDataFrame = WhyNotProvenance.prepareMSRsWithAlternatives(referenceScenario, whyNotQuestion)
    currentDataFrame
  }

  def extendedScenarioWithPreparedSAandMSR() : DataFrame = {
    WhyNotProvenance.computePreparedMSRsWithAlternatives(currentDataFrame)
  }



  def toCSV(iteration: Int, executionTime: Long): String = {
    val builder = scala.collection.mutable.StringBuilder.newBuilder
    builder.append(toCSV())
    builder.append(iteration)
    builder.append(";")
    builder.append(executionTime)
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

  def computeAlternatives(backtracedWhyNotQuestion: SchemaSubsetTree, input: LeafNode): PrimarySchemaSubsetTree = {
    PrimarySchemaSubsetTree(backtracedWhyNotQuestion)
  }

  def createAlternatives(primarySchemaSubsetTree: PrimarySchemaSubsetTree, numAlternatives: Int): Unit = {
    for (altIdx <- 0 until numAlternatives) {
      val alternative = SchemaSubsetTree()
      primarySchemaSubsetTree.addAlternative(alternative)
      primarySchemaSubsetTree.getRootNode.addAlternative(alternative.rootNode)
    }
    for (child <- primarySchemaSubsetTree.getRootNode.getChildren) {
      child.createDuplicates(100, primarySchemaSubsetTree.getRootNode, false, false)
    }
  }



}
