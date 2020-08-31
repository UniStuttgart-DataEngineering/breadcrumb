package de.uni_stuttgart.ipvs.provenance.transformations

import de.uni_stuttgart.ipvs.provenance.nested_why_not.{Constants, ProvenanceAttribute, ProvenanceContext, Rewrite, WhyNotPlanRewriter}
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.{SchemaAlternativesExpressionAlternatives, SchemaSubsetTree, SchemaSubsetTreeBackTracing}
import de.uni_stuttgart.ipvs.provenance.why_not_question.{SchemaBackTrace, SchemaBackTraceNew}
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, CaseWhen, EqualTo, Expression, GreaterThan, IsNotNull, IsNull, LessThanOrEqual, Literal, NamedExpression, Not, Or, Rand, Size}
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan, Project}
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.catalyst.plans.{Cross, FullOuter, Inner, LeftOuter, RightOuter}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

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

  def caseHandlingForNullValuesValid(leftColumn: NamedExpression, rightColumn: NamedExpression) : Expression = {
    val elseValue: Option[Expression] = Some(And(leftColumn, rightColumn))
    val leftAndNull = (And(leftColumn, IsNull(rightColumn)), leftColumn)
    val rightAndNull = (And(IsNull(leftColumn), rightColumn), rightColumn)
    val branches: Seq[(Expression, Expression)] = Seq(leftAndNull, rightAndNull)
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

  def survivorColumnWithAlternatives(lastLeftSurvivorColumn: NamedExpression, lastRightSurvivorColumn: NamedExpression, joinCondition: Expression, altId: Int): (ProvenanceAttribute, NamedExpression) = {
    val survivorAttribute = ProvenanceAttribute(oid, Constants.getSurvivorFieldName(oid, altId), BooleanType)
    val conditionForNullHandling = caseHandlingForNullValuesValid(lastLeftSurvivorColumn, lastRightSurvivorColumn)
    val survivorExpression = Alias(conditionForNullHandling, survivorAttribute.attributeName)()
    (survivorAttribute, survivorExpression)
  }

  def survivorColumns(currentProvenanceContext: ProvenanceContext, lastLeftSurvivorColumns: Seq[NamedExpression], lastRightSurvivorColumns: Seq[NamedExpression], joinConditions: Seq[Expression]): Seq[NamedExpression] = {
    val alternativeIds = currentProvenanceContext.primarySchemaAlternative.getAllAlternatives().map(alt => alt.id)
    val alternatingFactor = lastRightSurvivorColumns.size
    val survivorColumns = mutable.ListBuffer.empty[NamedExpression]
    val provenanceAttributes = mutable.ListBuffer.empty[ProvenanceAttribute]
    for (((altId, joinCondition), idx) <- (alternativeIds zip joinConditions).zipWithIndex ) {

      val lastLeftSurvivor = lastLeftSurvivorColumns(idx/alternatingFactor)
      val lastRightSurvivor = lastRightSurvivorColumns(idx%alternatingFactor)
      val (provenanceAttribute, survivorExpression) = survivorColumnWithAlternatives(lastLeftSurvivor, lastRightSurvivor, joinCondition, altId)
      provenanceAttributes += provenanceAttribute
      survivorColumns += survivorExpression
    }
    currentProvenanceContext.addSurvivorAttributes(provenanceAttributes)
    survivorColumns
  }

  def compatibleColumnWithAlternatives(lastLeftCompatibleColumn: NamedExpression, lastRightCompatibleColumn: NamedExpression, altId: Int): (ProvenanceAttribute, NamedExpression) = {
    val compatibleExpression = caseHandlingForNullValues(And(lastLeftCompatibleColumn, lastRightCompatibleColumn))
    val provenanceAttribute = ProvenanceAttribute(oid, Constants.getCompatibleFieldName(oid, altId), BooleanType)
    val namedCompatibleExpression = Alias(compatibleExpression, provenanceAttribute.attributeName)()
    (provenanceAttribute, namedCompatibleExpression)
  }


  def compatibleColumns(currentProvenanceContext: ProvenanceContext, lastLeftCompatibleColumns: Seq[NamedExpression], lastRightCompatibleColumns: Seq[NamedExpression]): Seq[NamedExpression] = {
    val alternativeIds = currentProvenanceContext.primarySchemaAlternative.getAllAlternatives().map(alt => alt.id)
    val alternatingFactor = lastRightCompatibleColumns.size
    val compatibleColumns = mutable.ListBuffer.empty[NamedExpression]
    val provenanceAttributes = mutable.ListBuffer.empty[ProvenanceAttribute]
    for ((altId, idx) <- alternativeIds.zipWithIndex){
      val lastLeftCompatible = lastLeftCompatibleColumns(idx/alternatingFactor)
      val lastRightCompatible = lastRightCompatibleColumns(idx%alternatingFactor)
      val (provenanceAttribute, compatibleExpression) = compatibleColumnWithAlternatives(lastLeftCompatible, lastRightCompatible, altId)
      provenanceAttributes += provenanceAttribute
      compatibleColumns += compatibleExpression
    }
    currentProvenanceContext.addCompatibilityAttributes(provenanceAttributes)
    compatibleColumns
  }

  def validColumnWithAlternatives(lastLeftValidColumn: NamedExpression, lastRightValidColumn: NamedExpression, altId: Int): (ProvenanceAttribute, NamedExpression) = {
    val validExpression = caseHandlingForNullValuesValid(lastLeftValidColumn, lastRightValidColumn)
    val provenanceAttribute = ProvenanceAttribute(oid, Constants.getValidFieldName(altId), BooleanType)
    val namedValidExpression = Alias(validExpression, provenanceAttribute.attributeName)()
    (provenanceAttribute, namedValidExpression)
  }

  def validColumns(currentProvenanceContext: ProvenanceContext, lastLeftValidColumns: Seq[NamedExpression], lastRightValidColumns: Seq[NamedExpression]): Seq[NamedExpression] = {
    val alternativeIds = currentProvenanceContext.primarySchemaAlternative.getAllAlternatives().map(alt => alt.id)
    val alternatingFactor = lastRightValidColumns.size
    val validColumns = mutable.ListBuffer.empty[NamedExpression]
    val provenanceAttributes = mutable.ListBuffer.empty[ProvenanceAttribute]
    for ((altId, idx) <- alternativeIds.zipWithIndex){
      val lastLeftValid = lastLeftValidColumns(idx/alternatingFactor)
      val lastRightValid = lastRightValidColumns(idx%alternatingFactor)
      val (provenanceAttribute, namedValidExpression) = validColumnWithAlternatives(lastLeftValid, lastRightValid, altId)
      provenanceAttributes += provenanceAttribute
      validColumns += namedValidExpression
    }
    currentProvenanceContext.replaceValidAttributes(provenanceAttributes)
    validColumns
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
    val rewrittenJoinCondition = join.condition.getOrElse(Literal(false))
    val rewrittenJoin = Join(leftRewrite.plan, rightRewrite.plan, FullOuter, Some(rewrittenJoinCondition))
    //val rewrittenJoinCondition = rewriteJoinConditionToPreserveCompatibles(leftRewrite, rightRewrite)
    //val rewrittenJoin = Join(leftRewrite.plan, rightRewrite.plan, Cross, Some(rewrittenJoinCondition))



    val compatibleColumn = this.compatibleColumn(provenanceContext, leftRewrite, rightRewrite)
    val survivorColumn = this.survivorColumn(provenanceContext,
      provenanceContext.getExpressionFromProvenanceAttribute(leftRewrite.provenanceContext.getMostRecentCompatibilityAttribute().get, rewrittenJoin.output).get,
      provenanceContext.getExpressionFromProvenanceAttribute(rightRewrite.provenanceContext.getMostRecentCompatibilityAttribute().get, rewrittenJoin.output).get)

    val projectList = rewrittenJoin.output :+ compatibleColumn :+ survivorColumn
    val projection = Project(projectList, rewrittenJoin)

    join.inputSet

    Rewrite(projection, provenanceContext)
  }

  override protected[provenance] def undoLeftSchemaModifications(schemaSubsetTree: SchemaSubsetTree): SchemaSubsetTree = {
    SchemaSubsetTreeBackTracing(schemaSubsetTree, leftChild.plan.output, join.left.output, leftChild.plan.output).getInputTree()
  }

  override protected[provenance] def undoRightSchemaModifications(schemaSubsetTree: SchemaSubsetTree): SchemaSubsetTree = {
    SchemaSubsetTreeBackTracing(schemaSubsetTree, rightChild.plan.output, join.right.output, rightChild.plan.output).getInputTree()
  }

  def renameValidColumns(validColumns: Seq[NamedExpression], left: Boolean): Seq[NamedExpression] = {
    validColumns.map{
      attribute => {
          val attributeName = Constants.getValidFieldWithBinaryOperatorExtension(attribute.name, left)
          Alias(attribute, attributeName)()
        }
    }
  }

  def planWithOldValidFields(childRewrite: Rewrite, left: Boolean): (LogicalPlan, Seq[String]) = {
    val oldValidColumns = childRewrite.provenanceContext.getExpressionsFromProvenanceAttributes(childRewrite.provenanceContext.getValidAttributes(), childRewrite.plan.output)
    val remainingAttributes = childRewrite.plan.output.filterNot(attribute => oldValidColumns.contains(attribute))
    val renamedAttributes = renameValidColumns(oldValidColumns, left)
    val projectList = remainingAttributes ++ renamedAttributes
    val newPlan = Project(projectList, childRewrite.plan)
    (newPlan, renamedAttributes.map(attr => attr.name))
  }

  override def rewriteWithAlternatives(): Rewrite = {
    val leftRewrite = leftChild.rewriteWithAlternatives()
    val rightRewrite = rightChild.rewriteWithAlternatives()

    val provenanceContext = ProvenanceContext.mergeContext(leftRewrite.provenanceContext, rightRewrite.provenanceContext)
    val dummyJoin = Join(leftRewrite.plan, rightRewrite.plan, FullOuter, None)
    val (joinCondition, alternativeExpressions) = SchemaAlternativesExpressionAlternatives(provenanceContext.primarySchemaAlternative, dummyJoin, Seq(join.condition.get)).forwardTraceJoinExpression(join.condition.get)

    //TODO rename valid fields

    val (leftPlan, oldLeftValidAttributeNames) = planWithOldValidFields(leftRewrite, true)
    val (rightPlan, oldRightValidAttributeNames) = planWithOldValidFields(rightRewrite, false)

    val rewrittenJoin = Join(leftPlan, rightPlan, FullOuter, Some(joinCondition))

    //compatibles
    val leftCompatibleColumns = provenanceContext.getExpressionsFromProvenanceAttributes(leftRewrite.provenanceContext.getMostRecentCompatibilityAttributes(), rewrittenJoin.output)
    val rightCompatibleColumns = provenanceContext.getExpressionsFromProvenanceAttributes(rightRewrite.provenanceContext.getMostRecentCompatibilityAttributes(), rewrittenJoin.output)
    val compatibles = compatibleColumns(provenanceContext, leftCompatibleColumns, rightCompatibleColumns)

    //survivors
    val leftSurvivorColumns = provenanceContext.getExpressionsFromProvenanceAttributes(leftRewrite.provenanceContext.getMostRecentCompatibilityAttributes(), rewrittenJoin.output)
    val rightSurvivorColumns = provenanceContext.getExpressionsFromProvenanceAttributes(rightRewrite.provenanceContext.getMostRecentCompatibilityAttributes(), rewrittenJoin.output)
    val survivors = survivorColumns(provenanceContext, leftSurvivorColumns, rightSurvivorColumns, alternativeExpressions)

    //valids
    val lastLeftValidColumns = getAttributesByName(rewrittenJoin.output, oldLeftValidAttributeNames)
    val lastRightValidColumns = getAttributesByName(rewrittenJoin.output, oldRightValidAttributeNames)
    val projectList = mutable.ListBuffer[NamedExpression](rewrittenJoin.output: _*)
    projectList --= lastLeftValidColumns
    projectList --= lastRightValidColumns
    projectList ++= validColumns(provenanceContext, lastLeftValidColumns, lastRightValidColumns)

    projectList ++= compatibles
    projectList ++= survivors


    val rewrittenPlan = Project(projectList.toList, rewrittenJoin)
    Rewrite(rewrittenPlan, provenanceContext)

  }


}
