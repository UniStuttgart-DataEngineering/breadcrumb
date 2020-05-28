package de.uni_stuttgart.ipvs.provenance.transformations
import de.uni_stuttgart.ipvs.provenance.nested_why_not.{Constants, ProvenanceAttribute, Rewrite, WhyNotPlanRewriter}
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.SchemaSubsetTree
import org.apache.spark.sql.catalyst.expressions.{Alias, CreateNamedStruct, CreateStruct, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, CollectList, Count}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.analysis
import org.apache.spark.sql.catalyst.expressions.aggregate.Complete
import org.apache.spark.sql.types.BooleanType

object AggregateRewrite {
  def apply(aggregate: Aggregate, whyNotQuestion:SchemaSubsetTree, oid: Int)  = new AggregateRewrite(aggregate, whyNotQuestion, oid)
}

class AggregateRewrite (aggregate: Aggregate, override val whyNotQuestion: SchemaSubsetTree, override val oid: Int) extends TransformationRewrite(aggregate, whyNotQuestion, oid){

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


    val previousProvenance = Alias(CollectList(Alias(CreateNamedStruct(provenanceContext.getExpressionFromAllProvenanceAttributes(rewrittenChild.output)), "NestedProvenanceTuple")()), "NestedProvenanceCollection")()
    val provenanceTuple = Alias(CreateStruct(rewrittenChild.output)/*provenanceContext.getExpressionFromAllProvenanceAttributes(rewrittenChild.output))*/, "NestedProvenanceTuple")()
    val provenanceProjection = Project(rewrittenChild.output :+ provenanceTuple , rewrittenChild)

    val otherAggregation = Alias(AggregateExpression(CollectList(getExpressionFromName(provenanceProjection, "NestedProvenanceTuple").get), Complete, false), "NestedProvenanceCollection")()
    val groupingExpressions = aggregate.groupingExpressions
    val aggregateExpressions = aggregate.aggregateExpressions :+ otherAggregation //previousProvenance

    val aggregateExpression = Aggregate(groupingExpressions, aggregateExpressions, provenanceProjection)

    //TODO: update provenanceContext
    Rewrite(aggregateExpression, provenanceContext)
  }

  def getExpressionFromName(operator: LogicalPlan, name: String): Option[NamedExpression] = {
    operator.output.find(attr => attr.name == name)
  }



}


