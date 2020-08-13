package de.uni_stuttgart.ipvs.provenance.transformations

import de.uni_stuttgart.ipvs.provenance.nested_why_not.Constants._
import de.uni_stuttgart.ipvs.provenance.nested_why_not.{Constants, ProvenanceAttribute, Rewrite, WhyNotPlanRewriter}
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.{SchemaAlternativesForwardTracing, SchemaSubsetTree, SchemaSubsetTreeBackTracing}
import de.uni_stuttgart.ipvs.provenance.why_not_question.SchemaBackTraceNew
import org.apache.spark.sql.catalyst.expressions.{Alias, And, AttributeReference, Expression, NamedExpression, Not, Or}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types.BooleanType

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object FilterRewrite {
  def apply(filter: Filter, oid: Int)  = new FilterRewrite(filter: Filter, oid: Int)
}

class FilterRewrite(filter: Filter, oid: Int) extends UnaryTransformationRewrite(filter, oid) {

  override def rewrite: Rewrite = {
    //val childRewrite = WhyNotPlanRewriter.rewrite(filter.child, SchemaBackTrace(filter, whyNotQuestion).unrestructure().head)
    val childRewrite = child.rewrite()


    val provenanceContext = childRewrite.provenanceContext
    val rewrittenChild = childRewrite.plan

    val projectList = filter.output ++
      provenanceContext.getExpressionFromAllProvenanceAttributes(rewrittenChild.output) ++
      provenanceAttributes(childRewrite)
    val rewrittenFilter = Project(
      projectList,
      childRewrite.plan
    )

    Rewrite(rewrittenFilter, childRewrite.provenanceContext)
  }

  override def rewriteWithAlternatives(): Rewrite = {
    val childRewrite = child.rewriteWithAlternatives()
    val provenanceContext = childRewrite.provenanceContext
    val rewrittenChild = childRewrite.plan

    val projectList = filter.output ++
      provenanceContext.getExpressionFromAllProvenanceAttributes(rewrittenChild.output) ++
      compatibleColumns(childRewrite) ++
      survivorColumns(childRewrite)

    val rewrittenFilter = Project(
      projectList,
      childRewrite.plan
    )
    Rewrite(rewrittenFilter, childRewrite.provenanceContext)

  }

  def compatibleColumns(rewrite: Rewrite): Seq[NamedExpression] = {
    val provenanceContext = rewrite.provenanceContext
    val mostRecentCompatibleAttributes = mutable.ListBuffer.empty[ProvenanceAttribute]
    val namedExpressions = mutable.ListBuffer.empty[NamedExpression]
    for (alternative <- provenanceContext.primarySchemaAlternative.getAllAlternatives()){
      val lastCompatibleAttribute = getPreviousCompatible(rewrite, alternative.id)
      val attributeName = getCompatibleFieldName(oid, alternative.id)
      val provenanceAttribute = ProvenanceAttribute(oid, attributeName, BooleanType)
      mostRecentCompatibleAttributes += provenanceAttribute
      namedExpressions += Alias(lastCompatibleAttribute, attributeName)()
    }
    provenanceContext.addCompatibilityAttributes(mostRecentCompatibleAttributes.toList)
    namedExpressions.toList
  }

  def survivorColumns(rewrite: Rewrite): Seq[NamedExpression] = {
    val provenanceContext = rewrite.provenanceContext
    val mostRecentSurvivorAttributes = mutable.ListBuffer.empty[ProvenanceAttribute]
    val namedExpressions = mutable.ListBuffer.empty[NamedExpression]
    val attributeName = Constants.getSurvivorFieldName(oid, provenanceContext.primarySchemaAlternative.id)
    val provenanceAttribute = ProvenanceAttribute(oid, attributeName, BooleanType)
    mostRecentSurvivorAttributes += provenanceAttribute
    namedExpressions += Alias(filter.condition, attributeName)()
    val alternativeExpressions = SchemaAlternativesForwardTracing(provenanceContext.primarySchemaAlternative, rewrite.plan, Seq(filter.condition)).forwardTraceExpression(filter.condition)

    for ((alternative, expression) <- provenanceContext.primarySchemaAlternative.alternatives zip alternativeExpressions){
      val attributeName = Constants.getSurvivorFieldName(oid, alternative.id)
      val provenanceAttribute = ProvenanceAttribute(oid, attributeName, BooleanType)
      mostRecentSurvivorAttributes += provenanceAttribute
      namedExpressions += Alias(expression, attributeName)()
    }
    provenanceContext.addSurvivorAttributes(mostRecentSurvivorAttributes.toList)
    namedExpressions.toList
  }

  def provenanceAttributes(rewrite: Rewrite): Seq[NamedExpression] = {
    val attributesToBeAdded = ListBuffer.empty[NamedExpression]
    attributesToBeAdded += survivorColumn(rewrite)
    attributesToBeAdded += compatibleColumn(rewrite)
    attributesToBeAdded.toList
  }

  //TODO maybe move to abstract class, change to provenance class
  def compatibleColumn(rewrite: Rewrite): NamedExpression = {
    val lastCompatibleAttribute = getPreviousCompatible(rewrite)
    val attributeName = addCompatibleAttributeToProvenanceContext(rewrite.provenanceContext)
    Alias(lastCompatibleAttribute, attributeName)()
  }

  def survivorColumn(rewrite: Rewrite): NamedExpression = {
    val attributeName = Constants.getSurvivorFieldName(oid)
    rewrite.provenanceContext.addSurvivorAttribute(ProvenanceAttribute(oid, attributeName, BooleanType))
    Alias(filter.condition, attributeName)()
  }

  override protected[provenance] def undoSchemaModifications(schemaSubsetTree: SchemaSubsetTree): SchemaSubsetTree = {
    schemaSubsetTree.deepCopy()
  }



  //TODO: Add printing method




}
