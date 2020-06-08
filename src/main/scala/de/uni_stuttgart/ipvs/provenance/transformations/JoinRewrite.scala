package de.uni_stuttgart.ipvs.provenance.transformations

import de.uni_stuttgart.ipvs.provenance.nested_why_not.{Constants, ProvenanceAttribute, ProvenanceContext, Rewrite, WhyNotPlanRewriter}
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.SchemaSubsetTree
import org.apache.spark.sql.catalyst.expressions.{Alias, And, CaseWhen, Expression, GreaterThan, IsNotNull, IsNull, LessThanOrEqual, Literal, NamedExpression, Not, Or, Rand, Size}
import org.apache.spark.sql.catalyst.plans.logical.{Join, Project}
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.catalyst.plans.{FullOuter, Inner, LeftOuter, RightOuter}

object JoinRewrite {
  def apply(join: Join, whyNotQuestion:SchemaSubsetTree, oid: Int)  = new JoinRewrite(join, whyNotQuestion, oid)
}

class JoinRewrite (val join: Join, override val whyNotQuestion: SchemaSubsetTree, override val oid: Int) extends BinaryTransformationRewrite(join, whyNotQuestion, oid) {
  override def unrestructureLeft(): SchemaSubsetTree = {
    //TODO implement
    whyNotQuestion
  }

  override def unrestructureRight(): SchemaSubsetTree = {
    //TODO implement
    whyNotQuestion
  }

  def compatibleColumn(currentProvenanceContext: ProvenanceContext, leftRewrite: Rewrite, rightRewrite: Rewrite): NamedExpression = {
    val leftCompatibleColumn = getPreviousCompatible(leftRewrite)
    val rightCompatibleColumn = getPreviousCompatible(rightRewrite)
    val compatibleExpression = caseHandlingForNullValues(And(leftCompatibleColumn, rightCompatibleColumn))
    val attributeName = addCompatibleAttributeToProvenanceContext(currentProvenanceContext)
    Alias(compatibleExpression, attributeName)()
  }

  def caseHandlingForNullValues(condition: Expression) : Expression = {
    val elseValue: Option[Expression] = Some(Literal(false))
    val branches: Seq[(Expression, Expression)] = Seq(Tuple2(IsNotNull(condition), condition))
    CaseWhen(branches, elseValue)
  }

  def survivorColumn(currentProvenanceContext: ProvenanceContext, lastLeftCompatibleColumn: NamedExpression, lastRightCompatibleColumn: NamedExpression): NamedExpression = {
    val joinEvaluationCondition = join.joinType match {
      case Inner => {
        join.condition.get
      }
      case LeftOuter => {
        And(join.condition.get, IsNotNull(lastRightCompatibleColumn))
      }
      case RightOuter => {
        And(join.condition.get, IsNotNull(lastLeftCompatibleColumn))
      }
      case FullOuter => {
        And(join.condition.get, And(IsNotNull(lastLeftCompatibleColumn),  IsNotNull(lastRightCompatibleColumn)))
      }
    }

    val survivorAttribute = ProvenanceAttribute(oid, Constants.getSurvivorFieldName(oid), BooleanType)
    currentProvenanceContext.addSurvivorAttribute(survivorAttribute)
    val conditionForNullHandling = caseHandlingForNullValues(joinEvaluationCondition)
    Alias(conditionForNullHandling, survivorAttribute.attributeName)()
  }

  override def rewrite(): Rewrite = {

    //unrestructure whynotquestion
    //rewrite left child
    //rewrite right child
    //merge provenance structure
    //rewrite plan

    val leftWhyNotQuestion = unrestructureLeft()
    val rightWhyNotQuestion = unrestructureRight()
    val leftRewrite = WhyNotPlanRewriter.rewrite(join.left, leftWhyNotQuestion)
    val rightRewrite = WhyNotPlanRewriter.rewrite(join.right, rightWhyNotQuestion)
    val provenanceContext = ProvenanceContext.mergeContext(leftRewrite.provenanceContext, rightRewrite.provenanceContext)

    val rewrittenJoin = Join(leftRewrite.plan, rightRewrite.plan, FullOuter, join.condition)

    val compatibleColumn = this.compatibleColumn(provenanceContext, leftRewrite, rightRewrite)
    val survivorColumn = this.survivorColumn(provenanceContext,
      provenanceContext.getExpressionFromProvenanceAttribute(leftRewrite.provenanceContext.getMostRecentCompatibilityAttribute().get, rewrittenJoin.output).get,
      provenanceContext.getExpressionFromProvenanceAttribute(rightRewrite.provenanceContext.getMostRecentCompatibilityAttribute().get, rewrittenJoin.output).get)

    val projectList = rewrittenJoin.output :+ compatibleColumn :+ survivorColumn
    val projection = Project(projectList, rewrittenJoin)

    Rewrite(projection, provenanceContext)
  }
}
