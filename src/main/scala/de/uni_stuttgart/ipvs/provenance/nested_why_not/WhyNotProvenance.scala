package de.uni_stuttgart.ipvs.provenance.nested_why_not

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import de.uni_stuttgart.ipvs.provenance.nested_why_not.Constants._
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.SchemaSubsetTree
import de.uni_stuttgart.ipvs.provenance.why_not_question.{Schema, SchemaMatcher, Twig}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.{EqualTo, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.{BooleanType, StructField, StructType}

/**
 * Entry point for plan rewrites
 */
object WhyNotProvenance {

  def rewrite(dataFrame: DataFrame, whyNotTwig: Twig): DataFrame = {
    val execState = dataFrame.queryExecution
    val basePlan = execState.analyzed
    val schema = new Schema(dataFrame)
    val schemaMatcher = SchemaMatcher(whyNotTwig, schema)
    val schemaMatch = schemaMatcher.getCandidate().getOrElse(throw new MatchError("The why not question either does not match or matches multiple times on the given dataframe schema."))
    val schemaSubset = SchemaSubsetTree(schemaMatch, schema)
    val rewrite = WhyNotPlanRewriter.rewrite(basePlan, schemaSubset)
    import org.apache.spark.sql.catalyst.analysis.Analyzer

    var plan = rewrite.plan
    //val analyzer = basePlan.asInstanceOf[Analyzer]
    plan = dataFrame.sparkSession.sessionState.analyzer.executeAndCheck(rewrite.plan)
    val outputStruct = StructType(
      rewrite.plan.output.map(out => StructField(out.name, out.dataType))
    )
    new DataFrame(
      execState.sparkSession,
      plan,
      RowEncoder(outputStruct)
    )
  }

  //def getResolvedAttribute(plan: LogicalPlan)





}
