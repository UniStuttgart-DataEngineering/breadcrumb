package de.uni_stuttgart.ipvs.provenance.transformations

import de.uni_stuttgart.ipvs.provenance.nested_why_not.WhyNotPlanRewriter
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.SchemaSubsetTree
import org.apache.spark.sql.catalyst.plans.logical.{BinaryNode, LogicalPlan}

abstract class BinaryTransformationRewrite (val plan: LogicalPlan, val oid: Int) extends TransformationRewrite {

  val leftChild: TransformationRewrite = WhyNotPlanRewriter.buildRewriteTree(plan.children.head)
  val rightChild: TransformationRewrite = WhyNotPlanRewriter.buildRewriteTree(plan.children.last)

  def children: Seq[TransformationRewrite] = leftChild :: rightChild :: Nil

  override protected def backtraceChildrenWhyNotQuestion: Unit = {
    val leftWNQuestion = undoLeftSchemaModifications(whyNotQuestion)
    val rightWNQuestion = undoRightSchemaModifications(whyNotQuestion)
    leftChild.backtraceWhyNotQuestion(leftWNQuestion)
    rightChild.backtraceWhyNotQuestion(rightWNQuestion)
  }

  protected def undoLeftSchemaModifications(schemaSubsetTree: SchemaSubsetTree): SchemaSubsetTree
  protected def undoRightSchemaModifications(schemaSubsetTree: SchemaSubsetTree): SchemaSubsetTree



}
