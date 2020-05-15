package de.uni_stuttgart.ipvs.provenance.transformations

import de.uni_stuttgart.ipvs.provenance.nested_why_not.{AnnotationExtension, Constants, ProvenanceAttribute, ProvenanceContext, Rewrite, WhyNotPlanRewriter}
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.SchemaSubsetTree
import de.uni_stuttgart.ipvs.provenance.why_not_question.SchemaMatch
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions.{Alias, CaseWhen, Expression, LessThanOrEqual, Literal, NamedExpression, Rand}
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LocalRelation, Project}
import org.apache.spark.sql.types.{BooleanType, StructField}


object RelationRewrite {
  def apply(relation: LeafNode, whyNotQuestion:SchemaSubsetTree, oid: Int)  = new RelationRewrite(relation, whyNotQuestion, oid)
}

class RelationRewrite(relation: LeafNode, whyNotQuestion:SchemaSubsetTree, oid: Int) extends TransformationRewrite(relation, whyNotQuestion, oid){

  def compatibleColumn(provenanceContext: ProvenanceContext): NamedExpression = {
    //TODO integrate with unrestructured whyNot question
    val attributeName = Constants.getCompatibleFieldName(oid)
    provenanceContext.addCompatibilityAttribute(ProvenanceAttribute(oid, attributeName, BooleanType))
    val condition = LessThanOrEqual(Rand(Literal(42)), Literal(0.5))
    val branches: Seq[(Expression, Expression)] = Seq(Tuple2(condition, Literal(true)))
    val elseValue: Option[Expression] = Some(Literal(false))
    Alias(CaseWhen(branches, elseValue), attributeName)()
  }

  override def rewrite: Rewrite = {
    val provenanceContext = new ProvenanceContext()
    val projectList = relation.output :+ compatibleColumn(provenanceContext)
    val rewrittenLocalRelation = Project(
      projectList,
      relation
    )
    Rewrite(rewrittenLocalRelation, provenanceContext)
  }





}
