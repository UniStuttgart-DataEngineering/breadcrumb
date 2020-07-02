package de.uni_stuttgart.ipvs.provenance.transformations

import de.uni_stuttgart.ipvs.provenance.nested_why_not.WhyNotPlanRewriter
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.SchemaSubsetTree
import org.apache.spark.sql.catalyst.plans.logical.UnaryNode

abstract class UnaryTransformationRewrite(val plan: UnaryNode, val oid: Int) extends TransformationRewrite {
  val child: TransformationRewrite = WhyNotPlanRewriter.buildRewriteTree(plan.child)

  def children: Seq[TransformationRewrite] = child :: Nil

  override protected def backtraceChildrenWhyNotQuestion: Unit = {
    val inputWhyNotQuestion = undoSchemaModifications(whyNotQuestion)
    child.backtraceWhyNotQuestion(inputWhyNotQuestion)
  }

  protected def undoSchemaModifications(schemaSubsetTree: SchemaSubsetTree): SchemaSubsetTree

}
