package de.uni_stuttgart.ipvs.provenance.nested_why_not

import org.apache.spark.sql.DataFrame
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.SchemaSubsetTree
import de.uni_stuttgart.ipvs.provenance.transformations.TransformationRewrite
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

  protected[provenance] def internalSetup(dataFrame: DataFrame, whyNotTwig: Twig): TransformationRewrite  = {
    ProvenanceContext.initializeUDF(dataFrame)
    val execState = dataFrame.queryExecution
    val basePlan = execState.analyzed
    val schema = new Schema(dataFrame)
    val schemaMatcher = SchemaMatcher(whyNotTwig, schema)
    val schemaMatch = schemaMatcher.getCandidate().getOrElse(throw new MatchError("The why not question either does not match or matches multiple times on the given dataframe schema."))
    val schemaSubset = SchemaSubsetTree(schemaMatch, schema)
    val rewriteTree = WhyNotPlanRewriter.buildRewriteTree(basePlan)
    rewriteTree.backtraceWhyNotQuestion(schemaSubset)
    rewriteTree

  }


  protected[provenance] def internalRewrite(dataFrame: DataFrame, whyNotTwig: Twig): Rewrite = {
    val rewriteTree = internalSetup(dataFrame, whyNotTwig)
    rewriteTree.rewrite()
  }

  protected[provenance] def internalRewriteWithAlternatives(dataFrame: DataFrame, whyNotTwig: Twig): Rewrite = {
    val rewriteTree = internalSetup(dataFrame, whyNotTwig)
    rewriteTree.rewriteWithAlternatives()
  }

  protected[provenance] def dataFrameAndProvenanceContext(dataFrame: DataFrame, whyNotTwig: Twig, rewriteFunction: (DataFrame, Twig) => Rewrite = internalRewrite): (DataFrame, ProvenanceContext) = {
    val execState = dataFrame.queryExecution
    val rewrite = rewriteFunction(dataFrame, whyNotTwig)
    val plan = dataFrame.sparkSession.sessionState.analyzer.executeAndCheck(rewrite.plan)
    //plan.resolve(Seq("otherValue"), dataFrame.sparkSession.conf.res)
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

  def rewriteWithAlternatives(dataFrame: DataFrame, whyNotTwig: Twig): DataFrame = {
//    dataFrame.explain()
    dataFrameAndProvenanceContext(dataFrame, whyNotTwig, internalRewriteWithAlternatives)._1
  }





}
