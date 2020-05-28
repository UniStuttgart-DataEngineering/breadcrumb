package de.uni_stuttgart.ipvs.provenance.transformations

import de.uni_stuttgart.ipvs.provenance.nested_why_not.{Constants, ProvenanceAttribute, ProvenanceContext, Rewrite, WhyNotPlanRewriter}
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.SchemaSubsetTree
import de.uni_stuttgart.ipvs.provenance.why_not_question.SchemaMatch
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, AttributeReference, CaseWhen, Expression, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual, Literal, NamedExpression, Or, Rand}
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LocalRelation, Project}
import org.apache.spark.sql.types.{BooleanType, StructField}

import scala.collection.mutable


object RelationRewrite {
  def apply(relation: LeafNode, whyNotQuestion:SchemaSubsetTree, oid: Int)  = new RelationRewrite(relation, whyNotQuestion, oid)
}

class RelationRewrite(relation: LeafNode, whyNotQuestion:SchemaSubsetTree, oid: Int) extends TransformationRewrite(relation, whyNotQuestion, oid){

  def compatibleColumn(provenanceContext: ProvenanceContext): NamedExpression = {
    //TODO: integrate with unrestructured whyNot question
    val attributeName = Constants.getCompatibleFieldName(oid)
    provenanceContext.addCompatibilityAttribute(ProvenanceAttribute(oid, attributeName, BooleanType))
    val condition = LessThanOrEqual(Rand(Literal(42)), Literal(0.5))
    val elseValue: Option[Expression] = Some(Literal(false))
    val branches: Seq[(Expression, Expression)] = Seq(Tuple2(condition, Literal(true)))
    Alias(CaseWhen(branches, elseValue), attributeName)()

    /*
    // Collect attributes from base relation
    val AttrNameToRef = scala.collection.mutable.Map[String, Expression]()
    for (attr <- relation.output) {
      AttrNameToRef.put(attr.name, attr)
    }

    var condition = List[Expression]()
    var constant: String = null
//    val condition = LessThanOrEqual(Rand(Literal(42)), Literal(0.5))
//    val branches: Seq[(Expression, Expression)] = Seq(Tuple2(condition, Literal(true)))

    // Adapt the condition from the why-not question
    for (child <- whyNotQuestion.rootNode.children) {
      if (child.constraint.constraintString == "") {
        condition = condition :+ Literal(true)
      } else {
        val conditionInChild = child.constraint.constraintString

        if (conditionInChild.contains(">")) {
          constant = conditionInChild.substring(2)
          condition = condition :+ expressions.GreaterThan(AttrNameToRef.get(child.name).get, Literal(constant))
        } else if (conditionInChild.contains(">=")) {
          constant = conditionInChild.substring(3)
          condition = condition :+ GreaterThanOrEqual(AttrNameToRef.get(child.name).get, Literal(constant))
        } else if (conditionInChild.contains("<")) {
          constant = conditionInChild.substring(2)
          condition = condition :+ LessThan(AttrNameToRef.get(child.name).get, Literal(constant))
        } else if (conditionInChild.contains("<=")) {
          constant = conditionInChild.substring(3)
          condition = condition :+ LessThanOrEqual(AttrNameToRef.get(child.name).get, Literal(constant))
        } else if (conditionInChild.equals("=")) {
          constant = conditionInChild.substring(2)
//          condition = Equal(Literal(child.name), Literal(constant))
        }
      }
    }

//    val elseValue: Option[Expression] = Some(Literal(false))
//    Alias(CaseWhen(branches, elseValue), attributeName)()

      var conjunctCond: Expression = null
      if (condition.length == 1) {
        conjunctCond = condition.head
      } else {
        for (eachCond <- condition) {
          conjunctCond = Or(conjunctCond,eachCond)
        }
      }

      Alias(conjunctCond, attributeName) ()

     */
  }

  override def rewrite: Rewrite = {
    val provenanceContext = new ProvenanceContext()
    val projectList = relation.output :+ compatibleColumn(provenanceContext)
    val rewrittenLocalRelation = Project(
      projectList,
      relation
    )

    //TODO: resolve compatible columns

    Rewrite(rewrittenLocalRelation, provenanceContext)
  }





}
