package de.uni_stuttgart.ipvs.provenance.nested_why_not

import org.apache.spark.sql.DataFrame
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.SchemaSubsetTree
import de.uni_stuttgart.ipvs.provenance.why_not_question.{DataFetcherUDF, Schema, SchemaMatcher, Twig}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{StructField, StructType}

/**
 * Entry point for plan rewrites
 */
object WhyNotProvenance {

  def computeMSRs(dataFrame: DataFrame, whyNotTwig: Twig): DataFrame = {
    val (rewrittenDF, provenanceContext) = dataFrameAndProvenanceContext(dataFrame, whyNotTwig)
    WhyNotMSRComputation.computeMSR(rewrittenDF, provenanceContext)
  }

  protected[provenance] def internalRewrite(dataFrame: DataFrame, whyNotTwig: Twig): Rewrite = {
    ProvenanceContext.initializeUDF(dataFrame)
    val execState = dataFrame.queryExecution
    val basePlan = execState.analyzed
    val schema = new Schema(dataFrame)
    val schemaMatcher = SchemaMatcher(whyNotTwig, schema)
    val schemaMatch = schemaMatcher.getCandidate().getOrElse(throw new MatchError("The why not question either does not match or matches multiple times on the given dataframe schema."))
    val schemaSubset = SchemaSubsetTree(schemaMatch, schema)
    WhyNotPlanRewriter.rewrite(basePlan, schemaSubset)
  }

  protected[provenance] def dataFrameAndProvenanceContext(dataFrame: DataFrame, whyNotTwig: Twig): (DataFrame, ProvenanceContext) = {
    val execState = dataFrame.queryExecution
    val rewrite = internalRewrite(dataFrame, whyNotTwig)
    val plan = dataFrame.sparkSession.sessionState.analyzer.executeAndCheck(rewrite.plan)
    val outputStruct = StructType(
      rewrite.plan.output.map(out => StructField(out.name, out.dataType))
    )
    val df = new DataFrame(
      execState.sparkSession,
      plan,
      RowEncoder(outputStruct)
    )
    (df, rewrite.provenanceContext)

  }

  def rewrite(dataFrame: DataFrame, whyNotTwig: Twig): DataFrame = {
    dataFrameAndProvenanceContext(dataFrame, whyNotTwig)._1
  }





}
