package de.uni_stuttgart.ipvs.provenance.transformations

import de.uni_stuttgart.ipvs.provenance.schema_alternatives.SchemaSubsetTree
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan}


abstract class InputTransformationRewrite (val plan: LeafNode, val oid: Int) extends TransformationRewrite {

  def children: Seq[TransformationRewrite] = Seq.empty[TransformationRewrite]

  override protected def backtraceChildrenWhyNotQuestion: Unit = {
    undoSchemaModifications(whyNotQuestion)
  }

  protected def undoSchemaModifications(schemaSubsetTree: SchemaSubsetTree): SchemaSubsetTree

}
