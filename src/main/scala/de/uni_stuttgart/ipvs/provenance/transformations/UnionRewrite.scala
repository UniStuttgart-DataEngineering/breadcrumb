package de.uni_stuttgart.ipvs.provenance.transformations

import de.uni_stuttgart.ipvs.provenance.nested_why_not.{ProvenanceContext, Rewrite, WhyNotPlanRewriter}
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.SchemaSubsetTree
import org.apache.spark.sql.catalyst.expressions.{Alias, NamedExpression, Or}
import org.apache.spark.sql.catalyst.plans.logical.Union

object UnionRewrite {
  def apply(union: Union, whyNotQuestion:SchemaSubsetTree, oid: Int)  = new UnionRewrite(union, whyNotQuestion, oid)
}

class UnionRewrite(val union: Union, override val whyNotQuestion: SchemaSubsetTree, override val oid: Int) extends BinaryTransformationRewrite(union, whyNotQuestion, oid){

  override def unrestructureLeft(): SchemaSubsetTree = {
    whyNotQuestion.deepCopy()
  }

  override def unrestructureRight(): SchemaSubsetTree = {
    whyNotQuestion.deepCopy()
  }

  def compatibleColumn(currentProvenanceContext: ProvenanceContext, leftRewrite: Rewrite, rightRewrite: Rewrite): NamedExpression = {
    val leftCompatibleColumn = getPreviousCompatible(leftRewrite)
    val rightCompatibleColumn = getPreviousCompatible(rightRewrite)
    val compatibleExpression = Or(leftCompatibleColumn, rightCompatibleColumn)
    val attributeName = addCompatibleAttributeToProvenanceContext(currentProvenanceContext)
    Alias(compatibleExpression, attributeName)()
  }



  override def rewrite(): Rewrite = {
    val leftWhyNotQuestion = unrestructureLeft()
    val rightWhyNotQuestion = unrestructureRight()
    assert(union.children.size == 2, "union does not have exactly two children which are needed for the rewrite")
    val leftRewrite = WhyNotPlanRewriter.rewrite(union.children(0), leftWhyNotQuestion)
    val rightRewrite = WhyNotPlanRewriter.rewrite(union.children(1), rightWhyNotQuestion)
    val provenanceContext = ProvenanceContext.mergeContext(leftRewrite.provenanceContext, rightRewrite.provenanceContext)
    Rewrite(union, provenanceContext)
  }
}
