package de.uni_stuttgart.ipvs.provenance.transformations
import de.uni_stuttgart.ipvs.provenance.nested_why_not.{Constants, ProvenanceAttribute, ProvenanceContext, Rewrite, WhyNotPlanRewriter}
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.SchemaSubsetTree
import org.apache.spark.sql.catalyst.expressions.{Alias, CreateNamedStruct, CreateStruct, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, CollectList, Complete, Count, Max, Min}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.analysis
import org.apache.spark.sql.types.BooleanType

object AggregateRewrite {
  def apply(aggregate: Aggregate, whyNotQuestion:SchemaSubsetTree, oid: Int)  = new AggregateRewrite(aggregate, whyNotQuestion, oid)
}

class AggregateRewrite (aggregate: Aggregate, override val whyNotQuestion: SchemaSubsetTree, override val oid: Int) extends UnaryTransformationRewrite(aggregate, whyNotQuestion, oid){

  override def unrestructure(): SchemaSubsetTree = {
    //TODO: ReplaceStubWithRealAggregation
    whyNotQuestion
  }

  //TODO: Add revalidation of compatibles here, i.e. replace this stub with a proper implementation
  def getPreviousCompatible(rewrite: Rewrite): NamedExpression = {
    val attribute = rewrite.provenanceContext.getMostRecentCompatibilityAttribute()
      .getOrElse(throw new MatchError("Unable to find previous compatible structure in provenance structure"))
    val compatibleAttribute = rewrite.plan.output.find(ex => ex.name == attribute.attributeName)
      .getOrElse(throw new MatchError("Unable to find previous compatible structure in output of previous operator"))
    compatibleAttribute
  }

  //TODO: Add revalidation of compatibles here, i.e. replace this stub with a proper implementation
  def compatibleColumn(rewrite: Rewrite): NamedExpression = {
    val lastCompatibleAttribute = getPreviousCompatible(rewrite)
    val attributeName = Constants.getCompatibleFieldName(oid)
    rewrite.provenanceContext.addCompatibilityAttribute(ProvenanceAttribute(oid, attributeName, BooleanType))

    Alias(lastCompatibleAttribute, attributeName)()
  }


  override def rewrite(): Rewrite = {
    val childRewrite = WhyNotPlanRewriter.rewrite(aggregate.child, unrestructure())
    val rewrittenChild = childRewrite.plan
    val provenanceContext = childRewrite.provenanceContext //array-buffer


    //val provenanceTuple = Alias(CreateStruct(rewrittenChild.output)/*provenanceContext.getExpressionFromAllProvenanceAttributes(rewrittenChild.output))*/, "NestedProvenanceTuple")()
    val provenanceTuple = Alias(
      CreateStruct(provenanceContext.getExpressionFromAllProvenanceAttributes(rewrittenChild.output)),
      Constants.getProvenanceTupleFieldName(oid))()
    val provenanceProjection = Project(rewrittenChild.output :+ provenanceTuple , rewrittenChild)

    val provenanceLegacy = Alias(
      AggregateExpression(
        CollectList(getExpressionFromName(provenanceProjection,
          Constants.getProvenanceTupleFieldName(oid)).get),
        Complete, false),
      Constants.getProvenanceCollectionFieldName(oid))()

    val compatibleField = Alias(
      AggregateExpression(
        Max(provenanceContext.getExpressionFromProvenanceAttribute(
          provenanceContext.getMostRecentCompatibilityAttribute().get, rewrittenChild.output).get),
        Complete, false),
      Constants.getCompatibleFieldName(oid))()


    val groupingExpressions = aggregate.groupingExpressions
    val aggregateExpressions = aggregate.aggregateExpressions :+ provenanceLegacy :+ compatibleField //previousProvenance

    val aggregateExpression = Aggregate(groupingExpressions, aggregateExpressions, provenanceProjection)

    val aggregatedProvenanceAttribute = ProvenanceAttribute(oid, Constants.getProvenanceCollectionFieldName(oid), provenanceLegacy.dataType)
    val aggregateContext = ProvenanceContext(provenanceContext, aggregatedProvenanceAttribute)

    val compatibleAttribute = ProvenanceAttribute(oid, Constants.getCompatibleFieldName(oid), BooleanType)
    aggregateContext.addCompatibilityAttribute(compatibleAttribute)

    Rewrite(aggregateExpression, aggregateContext)
  }

  def getExpressionFromName(operator: LogicalPlan, name: String): Option[NamedExpression] = {
    operator.output.find(attr => attr.name == name)
  }


}


