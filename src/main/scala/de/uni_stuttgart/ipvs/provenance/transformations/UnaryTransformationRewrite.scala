package de.uni_stuttgart.ipvs.provenance.transformations

import de.uni_stuttgart.ipvs.provenance.schema_alternatives.SchemaSubsetTree
import de.uni_stuttgart.ipvs.provenance.why_not_question.SchemaBackTrace
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

abstract class UnaryTransformationRewrite(val plan: LogicalPlan, val whyNotQuestion: SchemaSubsetTree, val oid: Int) extends TransformationRewrite {

//  def unrestructure():SchemaSubsetTree

}
