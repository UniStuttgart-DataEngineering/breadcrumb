package de.uni_stuttgart.ipvs.provenance.transformations

import de.uni_stuttgart.ipvs.provenance.nested_why_not.{Constants, ProvenanceAttribute, ProvenanceContext, Rewrite, WhyNotPlanRewriter}
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.{SchemaSubsetTree, SchemaSubsetTreeModifications}
import de.uni_stuttgart.ipvs.provenance.why_not_question.{SchemaBackTrace, SchemaBackTraceNew}
import org.apache.spark.sql.catalyst.expressions.{Alias, And, CaseWhen, EqualTo, Expression, GreaterThan, IsNotNull, IsNull, LessThanOrEqual, Literal, NamedExpression, Not, Or, Rand, Size}
import org.apache.spark.sql.catalyst.plans.logical.{Join, Project}
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.catalyst.plans.{FullOuter, Inner, LeftOuter, RightOuter}

object JoinRewrite {
  def apply(join: Join, oid: Int)  = new JoinRewrite(join, oid)
}

class JoinRewrite (val join: Join, override val oid: Int) extends BinaryTransformationRewrite(join, oid) {

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



  def rewriteJoinConditionToPreserveCompatibles(leftRewrite: Rewrite, rightRewrite: Rewrite): Expression = {
    val condition = join.condition.getOrElse(Literal(false))
    val leftCompatible = getPreviousCompatible(leftRewrite)
    val rightCompatible = getPreviousCompatible(rightRewrite)
    val leftCompatibleCondition = EqualTo(leftCompatible, Literal(true))
    val rightCompatibleCondition = EqualTo(rightCompatible, Literal(true))
    Or(condition, And(leftCompatibleCondition, rightCompatibleCondition))
  }

  override def rewrite(): Rewrite = {
    //val leftRewrite = WhyNotPlanRewriter.rewrite(join.left, SchemaBackTrace(join, whyNotQuestion).unrestructure().head)
    //val rightRewrite = WhyNotPlanRewriter.rewrite(join.right, SchemaBackTrace(join, whyNotQuestion).unrestructure().last)
    val leftRewrite = leftChild.rewrite()
    val rightRewrite = rightChild.rewrite()

    val provenanceContext = ProvenanceContext.mergeContext(leftRewrite.provenanceContext, rightRewrite.provenanceContext)
    //val rewrittenJoinCondition = rewriteJoinConditionToPreserveCompatibles(leftRewrite, rightRewrite)

    val rewrittenJoinCondition = join.condition.getOrElse(Literal(false))
    val rewrittenJoin = Join(leftRewrite.plan, rightRewrite.plan, FullOuter, Some(rewrittenJoinCondition))

    val compatibleColumn = this.compatibleColumn(provenanceContext, leftRewrite, rightRewrite)
    val survivorColumn = this.survivorColumn(provenanceContext,
      provenanceContext.getExpressionFromProvenanceAttribute(leftRewrite.provenanceContext.getMostRecentCompatibilityAttribute().get, rewrittenJoin.output).get,
      provenanceContext.getExpressionFromProvenanceAttribute(rightRewrite.provenanceContext.getMostRecentCompatibilityAttribute().get, rewrittenJoin.output).get)

    val projectList = rewrittenJoin.output :+ compatibleColumn :+ survivorColumn
    val projection = Project(projectList, rewrittenJoin)

    Rewrite(projection, provenanceContext)
  }

  override protected[provenance] def undoLeftSchemaModifications(schemaSubsetTree: SchemaSubsetTree): SchemaSubsetTree = {
    SchemaSubsetTreeModifications(schemaSubsetTree, leftChild.plan.output, join.left.output, leftChild.plan.output).getInputTree()
  }

  override protected[provenance] def undoRightSchemaModifications(schemaSubsetTree: SchemaSubsetTree): SchemaSubsetTree = {
    SchemaSubsetTreeModifications(schemaSubsetTree, rightChild.plan.output, join.right.output, rightChild.plan.output).getInputTree()
  }

}
