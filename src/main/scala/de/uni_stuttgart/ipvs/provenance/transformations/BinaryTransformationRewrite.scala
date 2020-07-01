package de.uni_stuttgart.ipvs.provenance.transformations

import de.uni_stuttgart.ipvs.provenance.schema_alternatives.SchemaSubsetTree
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

abstract class BinaryTransformationRewrite (val plan: LogicalPlan, val whyNotQuestion: SchemaSubsetTree, val oid: Int) extends TransformationRewrite {

//  def unrestructureLeft():SchemaSubsetTree
//  def unrestructureRight():SchemaSubsetTree

}
