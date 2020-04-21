package de.uni_stuttgart.ipvs.provenance.nested_why_not

import org.apache.spark.sql.{Column, DataFrame}
import de.uni_stuttgart.ipvs.provenance.nested_why_not.Constants._
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

}
