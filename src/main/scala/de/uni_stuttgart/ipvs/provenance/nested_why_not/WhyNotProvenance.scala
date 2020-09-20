package de.uni_stuttgart.ipvs.provenance.nested_why_not

import org.apache.spark.sql.DataFrame
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.SchemaSubsetTree
import de.uni_stuttgart.ipvs.provenance.transformations.TransformationRewrite
import de.uni_stuttgart.ipvs.provenance.why_not_question.{DataFetcherUDF, Schema, SchemaMatcher, Twig}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.functions.typedLit

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
    var plan = dataFrame.sparkSession.sessionState.analyzer.executeAndCheck(rewrite.plan)
    //plan.resolve(Seq("otherValue"), dataFrame.sparkSession.conf.res)
    plan = Project(plan.output.distinct, plan)
    val outputStruct = StructType(
      rewrite.plan.output.map(out => StructField(out.name, out.dataType))
    )
    val df = new DataFrame(
      execState.sparkSession,
      plan,
      RowEncoder(outputStruct)
    )
    println("Alternatives: " + rewrite.provenanceContext.primarySchemaAlternative.getAllAlternatives().size)
    (df, rewrite.provenanceContext)

  }

  def rewrite(dataFrame: DataFrame, whyNotTwig: Twig): DataFrame = {
    dataFrameAndProvenanceContext(dataFrame, whyNotTwig)._1
  }

  def rewriteWithAlternatives(dataFrame: DataFrame, whyNotTwig: Twig): DataFrame = {
//    dataFrame.explain()
    dataFrameAndProvenanceContext(dataFrame, whyNotTwig, internalRewriteWithAlternatives)._1
  }

  def MSRsWithAlternatives(dataFrame: DataFrame, whyNotTwig: Twig): Map[Int, DataFrame] = {
    val (result, provenanceContext) = dataFrameAndProvenanceContext(dataFrame, whyNotTwig, internalRewriteWithAlternatives)
    val res = WhyNotMSRComputation.computeMSRForSchemaAlternatives(result, provenanceContext)
    res
  }

  def printMSRsWithAlternatives(dataFrame: DataFrame, whyNotTwig: Twig) = {
    val res = MSRsWithAlternatives(dataFrame, whyNotTwig)
    for ((id, pickyOps) <- res) {
      println("Schema Alternative: " + id)
      pickyOps.show(false)
    }
  }

  def computeMSRsWithAlternatives(dataFrame: DataFrame, whyNotTwig: Twig): DataFrame = {
    val res: Map[Int, DataFrame] = MSRsWithAlternatives(dataFrame, whyNotTwig)
    val (primaryAlternative, primaryDataFrame) = res.minBy(_._1)
    var aggregatedResults = primaryDataFrame.withColumn("alternative", typedLit(f"${primaryAlternative}%06d"))
    for ((alternative, altFrame) <- res) {
      if (alternative != primaryAlternative){
        val manipulatedDataFrame = altFrame.withColumn("alternative", typedLit(f"${alternative}%06d"))
        aggregatedResults = aggregatedResults.union(manipulatedDataFrame)
      }
    }
    aggregatedResults
  }

  def prepareMSRsWithAlternatives(dataFrame: DataFrame, whyNotTwig: Twig): DataFrame = {
    val (result, provenanceContext) = dataFrameAndProvenanceContext(dataFrame, whyNotTwig, internalRewriteWithAlternatives)
    val res = WhyNotMSRComputation.prepareForMSRComputationWithAlternatives(result, provenanceContext)
    res
  }

  def computePreparedMSRsWithAlternatives(dataFrame: DataFrame): DataFrame = {
    val res = WhyNotMSRComputation.computeMSRForSchemaAlternatives(dataFrame, WhyNotMSRComputation.currentProvenanceContext, true, WhyNotMSRComputation.currentUidAttribute)
    val (primaryAlternative, primaryDataFrame) = res.minBy(_._1)
    var aggregatedResults = primaryDataFrame.withColumn("alternative", typedLit(f"${primaryAlternative}%06d"))
    for ((alternative, altFrame) <- res) {
      if (alternative != primaryAlternative){
        val manipulatedDataFrame = altFrame.withColumn("alternative", typedLit(f"${alternative}%06d"))
        aggregatedResults = aggregatedResults.union(manipulatedDataFrame)
      }
    }
    aggregatedResults
  }

}
