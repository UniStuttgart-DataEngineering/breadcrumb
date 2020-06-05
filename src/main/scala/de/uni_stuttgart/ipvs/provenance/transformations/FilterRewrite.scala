package de.uni_stuttgart.ipvs.provenance.transformations

import de.uni_stuttgart.ipvs.provenance.nested_why_not.Constants._
import de.uni_stuttgart.ipvs.provenance.nested_why_not.{Constants, ProvenanceAttribute, Rewrite, WhyNotPlanRewriter}
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.SchemaSubsetTree
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Expression, NamedExpression, Not, Or}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project}
import org.apache.spark.sql.types.BooleanType

import scala.collection.mutable.ListBuffer

object FilterRewrite {
  def apply(filter: Filter, whyNotQuestion:SchemaSubsetTree, oid: Int)  = new FilterRewrite(filter: Filter, whyNotQuestion:SchemaSubsetTree, oid: Int)
}

class FilterRewrite(filter: Filter, whyNotQuestion:SchemaSubsetTree, oid: Int) extends TransformationRewrite(filter, whyNotQuestion, oid) {

  override def rewrite: Rewrite = {
    val childRewrite = WhyNotPlanRewriter.rewrite(filter.child, unrestructure())
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

  def provenanceAttributes(rewrite: Rewrite): Seq[NamedExpression] = {
    val attributesToBeAdded = ListBuffer.empty[NamedExpression]
    attributesToBeAdded += survivorColumn(rewrite)
    attributesToBeAdded += compatibleColumn(rewrite)
    attributesToBeAdded.toList
  }

  def getPreviousCompatible(rewrite: Rewrite): NamedExpression = {
    val attribute = rewrite.provenanceContext.getMostRecentCompatibilityAttribute()
      .getOrElse(throw new MatchError("Unable to find previous compatible structure in provenance structure"))
    val compatibleAttribute = rewrite.plan.output.find(ex => ex.name == attribute.attributeName)
      .getOrElse(throw new MatchError("Unable to find previous compatible structure in output of previous operator"))
    compatibleAttribute
  }

  def compatibleColumn(rewrite: Rewrite): NamedExpression = {
    val lastCompatibleAttribute = getPreviousCompatible(rewrite)
    val attributeName = Constants.getCompatibleFieldName(oid)
    rewrite.provenanceContext.addCompatibilityAttribute(ProvenanceAttribute(oid, attributeName, BooleanType))
    Alias(lastCompatibleAttribute, attributeName)()
  }

  def survivorColumn(rewrite: Rewrite): NamedExpression = {
    val attributeName = Constants.getSurvivorFieldName(oid)
    rewrite.provenanceContext.addSurvivorAttribute(ProvenanceAttribute(oid, attributeName, BooleanType))
    Alias(filter.condition, attributeName)()
  }
}
