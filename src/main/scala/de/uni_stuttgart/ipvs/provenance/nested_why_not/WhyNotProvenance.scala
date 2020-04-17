package de.uni_stuttgart.ipvs.provenance.nested_why_not

import org.apache.spark.sql.DataFrame
import de.uni_stuttgart.ipvs.provenance.nested_why_not.Constants._
import org.apache.spark.sql.catalyst.encoders.RowEncoder

/**
 * Entry point for plan rewrites
 */
object WhyNotProvenance {

  def annotate(dataFrame: DataFrame): DataFrame = {
    val execState = dataFrame.queryExecution
    val plan = execState.analyzed
    val annotated = WhyNotPlanRewriter(plan)
    val baseSchema = plan.schema
    val annotatedSchema = if (baseSchema.fields.exists(_.name == PROVENANCE_ID_STRUCT))
      baseSchema else
      baseSchema.add(PROVENANCE_ID_STRUCT, AnnotationEncoder().annotationStruct())
    new DataFrame(
      execState.sparkSession,
      annotated,
      RowEncoder(annotatedSchema)
    )
  }

}
