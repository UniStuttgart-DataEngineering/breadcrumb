package de.uni_stuttgart.ipvs.provenance.transformations

import de.uni_stuttgart.ipvs.provenance.nested_why_not.{ProvenanceAttribute, ProvenanceContext, Rewrite}
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.{SchemaSubsetTree, SchemaSubsetTreeModifications}
import de.uni_stuttgart.ipvs.provenance.why_not_question.SchemaBackTraceNew
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Cast, Expression, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, Union}
import org.apache.spark.sql.execution.datasources.LogicalRelation

object UnionRewrite {
  def apply(union: Union, oid: Int)  = new UnionRewrite(union, oid)
}

class UnionRewrite(val union: Union, override val oid: Int) extends BinaryTransformationRewrite(union, oid){

  lazy val attributeMapping = leftChild.plan.output.zip(union.output).toMap

  def compatibleColumn( rewrite: Rewrite, attributeName: String): NamedExpression = {
    val compatibleExpression = getPreviousCompatible(rewrite)
    Alias(compatibleExpression, attributeName)()
  }

  def generateNullColumns(attributes: Seq[ProvenanceAttribute]): Seq[NamedExpression] = {
    attributes.map { attribute =>
      Alias(Cast(Literal(null), attribute.attributeType), attribute.attributeName)()
    }
  }


  def getLeftProvenanceProjection(leftRewrite: Rewrite, rightRewrite: Rewrite, attributeName: String): LogicalPlan = {
    val projectionLists = getProjectionLists(leftRewrite, rightRewrite)
    val compatibleColumn = this.compatibleColumn(leftRewrite, attributeName)
    Project(
      projectionLists.nonProvenanceOutput ++ projectionLists.provenanceOutput ++ projectionLists.nullColumns :+ compatibleColumn
      , leftRewrite.plan)
  }

  def getRightProvenanceProjection(leftRewrite: Rewrite, rightRewrite: Rewrite, attributeName: String): LogicalPlan = {
    val projectionLists = getProjectionLists(rightRewrite, leftRewrite)
    val compatibleColumn = this.compatibleColumn(rightRewrite, attributeName)
    Project(
      projectionLists.nonProvenanceOutput ++ projectionLists.nullColumns ++ projectionLists.provenanceOutput :+ compatibleColumn
      , rightRewrite.plan)
  }

  case class ProjectionLists(nonProvenanceOutput: Seq[Attribute], provenanceOutput: Seq[Attribute], nullColumns: Seq[NamedExpression])

  protected def getProjectionLists(projectionRewrite: Rewrite, otherRewrite: Rewrite): ProjectionLists = {
    val plan = projectionRewrite.plan
    val context = projectionRewrite.provenanceContext
    val NullCols = generateNullColumns(otherRewrite.provenanceContext.provenanceAttributes).sortBy(a => a.name)
    val nonProvenanceOutput = plan.output.filter(col => !context.isProvenanceAttribute(col))
    val provenanceOutput = plan.output.filter(col => context.isProvenanceAttribute(col)).sortBy(a => a.name)
    ProjectionLists(nonProvenanceOutput, provenanceOutput, NullCols)
  }

  override def rewrite(): Rewrite = {
//    val leftWhyNotQuestion = unrestructureLeft()
//    val rightWhyNotQuestion = unrestructureRight()
    assert(union.children.size == 2, "union does not have exactly two children which are needed for the rewrite")
//    val leftRewrite = WhyNotPlanRewriter.rewrite(union.children(0), leftWhyNotQuestion)
//    val rightRewrite = WhyNotPlanRewriter.rewrite(union.children(1), rightWhyNotQuestion)

    //val leftRewrite = WhyNotPlanRewriter.rewrite(union.children(0), SchemaBackTrace(union, whyNotQuestion).unrestructure().head)
    //val rightRewrite = WhyNotPlanRewriter.rewrite(union.children(1), SchemaBackTrace(union, whyNotQuestion).unrestructure().last)
    val leftRewrite = leftChild.rewrite()
    val rightRewrite = rightChild.rewrite()
    val provenanceContext = ProvenanceContext.mergeContext(leftRewrite.provenanceContext, rightRewrite.provenanceContext)
    val newCompatibleAttribute = addCompatibleAttributeToProvenanceContext(provenanceContext)
    val leftPlan = getLeftProvenanceProjection(leftRewrite, rightRewrite, newCompatibleAttribute)
    val rightPlan = getRightProvenanceProjection(leftRewrite, rightRewrite, newCompatibleAttribute)
    val rewrittenUnion = Union(leftPlan, rightPlan)
    Rewrite(rewrittenUnion, provenanceContext)
  }

  override protected[provenance] def undoLeftSchemaModifications(schemaSubsetTree: SchemaSubsetTree): SchemaSubsetTree = {
    schemaSubsetTree.deepCopy()
  }

  override protected[provenance] def undoRightSchemaModifications(schemaSubsetTree: SchemaSubsetTree): SchemaSubsetTree = {
    SchemaSubsetTreeModifications(schemaSubsetTree, rightChild.plan.output, union.output, rightChild.plan.output).backtraceUnion()
    //TODO: make a deep check on attribute names
  }





}
