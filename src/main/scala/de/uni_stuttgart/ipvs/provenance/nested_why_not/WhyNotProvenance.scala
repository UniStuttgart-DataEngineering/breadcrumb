package de.uni_stuttgart.ipvs.provenance.nested_why_not

import org.apache.spark.sql.{Column, DataFrame}
import de.uni_stuttgart.ipvs.provenance.nested_why_not.Constants._
import de.uni_stuttgart.ipvs.provenance.why_not_question.{Schema, SchemaMatcher, Twig}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.{EqualTo, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.{BooleanType, StructField, StructType}

/**
 * Entry point for plan rewrites
 */
object WhyNotProvenance {

  def annotate(dataFrame: DataFrame, pq: Expression): DataFrame = {
    val execState = dataFrame.queryExecution
    val basePlan = execState.analyzed
    val baseSchema = basePlan.schema
    val annotatedPlan = WhyNotPlanRewriter(basePlan, pq)
//    var annotatedSchema = if (baseSchema.fields.exists(_.name == PROVENANCE_ID_STRUCT))
//      baseSchema else
//      baseSchema.add(PROVENANCE_ID_STRUCT, AnnotationEncoder().annotationStruct())

    // TODO: add all additional fields
    var annotatedSchema = baseSchema

    for (attr <- annotatedPlan.output) {
      if (annotatedSchema.fieldNames == PROVENANCE_ID_STRUCT) {
        annotatedSchema = annotatedSchema.add(PROVENANCE_ID_STRUCT, AnnotationEncoder().annotationStruct())
      }
      else if (!annotatedSchema.names.contains(attr.name)) {
        annotatedSchema = annotatedSchema.add(attr.name, BooleanType)
      }
    }
//    print(annotatedSchema)

    new DataFrame(
      execState.sparkSession,
      annotatedPlan,
      RowEncoder(annotatedSchema)
    )
  }

  def rewrite(dataFrame: DataFrame, whyNotTwig: Twig): DataFrame = {
    val execState = dataFrame.queryExecution
    val basePlan = execState.analyzed
    val schema = new Schema(dataFrame)
    val schemaMatcher = SchemaMatcher(whyNotTwig, schema)
    val schemaMatch = schemaMatcher.getCandidate().getOrElse(throw new MatchError("The why not question either does not match or matches multiple times on the given dataframe schema."))
    val rewrite = WhyNotPlanRewriter.rewrite(basePlan, schemaMatch)
    val outputStruct = StructType(
      rewrite.plan.output.map(out => StructField(out.name, out.dataType))
    )

    new DataFrame(
      execState.sparkSession,
      rewrite.plan,
      RowEncoder(outputStruct)
    )


  }



}
