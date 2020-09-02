package de.uni_stuttgart.ipvs.provenance.transformations
import de.uni_stuttgart.ipvs.provenance.nested_why_not.{Constants, ProvenanceAttribute, ProvenanceContext, Rewrite, WhyNotPlanRewriter}
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.{SchemaAlternativesExpressionAlternatives, SchemaAlternativesForwardTracing, SchemaSubsetTree, SchemaSubsetTreeAccessAdder, SchemaSubsetTreeBackTracing}
import de.uni_stuttgart.ipvs.provenance.why_not_question.SchemaBackTrace
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, AttributeReference, CreateNamedStruct, CreateStruct, EqualTo, Expression, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, CollectList, Complete, Count, Max, Min}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Expand, GroupingSets, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.analysis
import org.apache.spark.sql.types.{BooleanType, IntegerType}

import scala.collection.mutable

object AggregateRewrite {
  def apply(aggregate: Aggregate, oid: Int)  = new AggregateRewrite(aggregate, oid)
}

class AggregateRewrite (aggregate: Aggregate, override val oid: Int) extends UnaryTransformationRewrite(aggregate, oid){

  //TODO: Add revalidation of compatibles here, i.e. replace this stub with a proper implementation
  def compatibleColumn(rewrite: Rewrite): NamedExpression = {
    val lastCompatibleAttribute = getPreviousCompatible(rewrite)
    val attributeName = Constants.getCompatibleFieldName(oid)
    rewrite.provenanceContext.addCompatibilityAttribute(ProvenanceAttribute(oid, attributeName, BooleanType))

    Alias(lastCompatibleAttribute, attributeName)()
  }

  var outputWhyNotQuestion: SchemaSubsetTree = null


  override def rewrite(): Rewrite = {
    //val childRewrite = WhyNotPlanRewriter.rewrite(aggregate.child, SchemaBackTrace(aggregate, whyNotQuestion).unrestructure().head)
    val childRewrite = child.rewrite()
    val rewrittenChild = childRewrite.plan
    val provenanceContext = childRewrite.provenanceContext //array-buffer

    //val provenanceTuple = Alias(CreateStruct(rewrittenChild.output)/*provenanceContext.getExpressionFromAllProvenanceAttributes(rewrittenChild.output))*/, "NestedProvenanceTuple")()
    val provenanceTuple = getProvenanceTuple(childRewrite)
    val provenanceProjection = Project(rewrittenChild.output :+ provenanceTuple , rewrittenChild)

    val provenanceLegacy = getNestedProvenanceCollection(provenanceProjection)

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

  def getProvenanceTuple(rewrite: Rewrite): NamedExpression = {
    Alias(
      CreateStruct(rewrite.provenanceContext.getExpressionFromAllProvenanceAttributes(rewrite.plan.output)),
      Constants.getProvenanceTupleFieldName(oid))()
  }

  def getNestedProvenanceCollection(child: LogicalPlan): NamedExpression = {
    Alias(
      AggregateExpression(
        CollectList(getExpressionFromName(child,
          Constants.getProvenanceTupleFieldName(oid)).get),
        Complete, false),
      Constants.getProvenanceCollectionFieldName(oid))()
  }

  def getExpressionFromName(operator: LogicalPlan, name: String): Option[NamedExpression] = {
    operator.output.find(attr => attr.name == name)
  }

  override protected[provenance] def undoSchemaModifications(schemaSubsetTree: SchemaSubsetTree): SchemaSubsetTree = {
    outputWhyNotQuestion = schemaSubsetTree
    var inputTree = SchemaSubsetTreeBackTracing(schemaSubsetTree, child.plan.output, aggregate.output, aggregate.aggregateExpressions).getInputTree()
    inputTree = SchemaSubsetTreeAccessAdder(inputTree, aggregate.aggregateExpressions).traceAttributeAccess()
    inputTree

  }

  def getExpandAttributeName: String = {
    "spark_grouping_id"
  }

  def getValidAggregations(plan: LogicalPlan, provenanceContext: ProvenanceContext): (Seq[NamedExpression], Seq[String]) = {
    val oldValidColumns = provenanceContext.getExpressionsFromProvenanceAttributes(provenanceContext.getValidAttributes(), plan.output)
    val aggregateExpressions = mutable.ListBuffer.empty[NamedExpression]
    val validNames = mutable.ListBuffer.empty[String]
    for (validColumn <- oldValidColumns){
      val (expression, oldName) = getValidAggregation(validColumn)
      aggregateExpressions += expression
      validNames += oldName
    }
    (aggregateExpressions.toList, validNames.toList)
  }

  def getValidAggregation(attribute: NamedExpression): (NamedExpression, String) = {
    val oldName = Constants.getValidFieldWithOldExtension(attribute.name)
    val expression = Alias(
    AggregateExpression(
      Max(attribute),
      Complete, false),
      oldName)()
    (expression, oldName)
  }

  def getValidColumn(oldValidColumn: NamedExpression, groupingIDColumn: NamedExpression, alternativeId: Int): NamedExpression = {
    val validName = Constants.getValidFieldName(alternativeId)
    val matchExpression = EqualTo(groupingIDColumn, Literal(alternativeId, IntegerType))
    val validCondition = And(oldValidColumn, matchExpression)
    Alias(validCondition, validName)()
  }

  def getValidColumns(plan: LogicalPlan, provenanceContext: ProvenanceContext, oldValidNames: Seq[String]): Seq[NamedExpression] = {
    val groupingIDColumn = getAttributeByName(plan.output, getExpandAttributeName).get
    val validColumns = mutable.ListBuffer.empty[NamedExpression]
    for ((alternative, oldValidName) <- provenanceContext.primarySchemaAlternative.getAllAlternatives() zip oldValidNames) {
      val oldValidColumn = getAttributeByName(plan.output, oldValidName).get
      validColumns += getValidColumn(oldValidColumn, groupingIDColumn, alternative.id)
    }
    validColumns.toList
  }

  def getProjections(context: ProvenanceContext, aggregateExpressions: Seq[NamedExpression], groupingExpressions: Seq[NamedExpression]): (Seq[Seq[Expression]], Seq[Attribute]) = {
    val extendedAggregateExpressions = aggregateExpressions :+ AttributeReference(getExpandAttributeName, IntegerType, false)()
    val groupedAggregateExpressions = mutable.ListBuffer.empty[Seq[Expression]]
    val moduloFactor = context.primarySchemaAlternative.getAllAlternatives().size
    for ((alternative, altIdx) <- context.primarySchemaAlternative.getAllAlternatives().zipWithIndex){
      val outputAggregateAttributes = mutable.ListBuffer.empty[Expression]
      for(idx <- 0 until groupingExpressions.size){
        if (idx % moduloFactor == altIdx){
          outputAggregateAttributes += aggregateExpressions(idx)
        } else {
          outputAggregateAttributes += Literal(null, aggregateExpressions(idx).dataType)
        }
      }
      for(idx <- groupingExpressions.size until aggregateExpressions.size) {
        outputAggregateAttributes += aggregateExpressions(idx)
      }
      outputAggregateAttributes += Literal(alternative.id, IntegerType)
      groupedAggregateExpressions += outputAggregateAttributes.toList
    }
    (groupedAggregateExpressions.toList, extendedAggregateExpressions.map{expr => expr.asInstanceOf[Attribute]})
  }

  override def rewriteWithAlternatives(): Rewrite = {

    val childRewrite = child.rewriteWithAlternatives()
    val rewrittenChild = childRewrite.plan
    val provenanceContext = childRewrite.provenanceContext

    //TODO: provenance context

    val provenanceTuple = getProvenanceTuple(childRewrite)
    val provenanceProjection = Project(rewrittenChild.output :+ provenanceTuple , rewrittenChild)
    val provenanceLegacy = getAttributeByName(provenanceProjection.output, provenanceTuple.name)

    val validColumns = provenanceContext.getExpressionsFromProvenanceAttributes(provenanceContext.getValidAttributes(), provenanceProjection.output)
    val provenanceColumns = validColumns ++ provenanceLegacy

    //val aggregateExpressions = SchemaAlternativesExpressionAlternatives(provenanceContext.primarySchemaAlternative, rewrittenChild, aggregate.aggregateExpressions).forwardTraceNamedExpressions()
    var aggregateExpressionWithoutAggregate = SchemaAlternativesExpressionAlternatives(provenanceContext.primarySchemaAlternative, provenanceProjection, aggregate.aggregateExpressions).isForGroupingSets().forwardTraceNamedExpressions()
    aggregateExpressionWithoutAggregate = aggregateExpressionWithoutAggregate ++ provenanceColumns
    val groupingExpressions = SchemaAlternativesExpressionAlternatives(provenanceContext.primarySchemaAlternative, provenanceProjection, aggregate.groupingExpressions).forwardTraceNamedExpressions()



    val (projections, output) = getProjections(provenanceContext, aggregateExpressionWithoutAggregate, groupingExpressions)
    val expand = Expand(projections, output, provenanceProjection)


    val provenanceCollection = getNestedProvenanceCollection(expand)
    val aggregateExpressionAfterExpand = SchemaAlternativesExpressionAlternatives(provenanceContext.primarySchemaAlternative, expand, aggregate.aggregateExpressions).forwardTraceNamedExpressions()
    val (validColumnsAfterRewrite, validNames) = getValidAggregations(expand, provenanceContext)
    val groupingColumn = getAttributeByName(expand.output, getExpandAttributeName)
    val extendedAggregateExpressionAfterExpand = aggregateExpressionAfterExpand ++ groupingColumn ++ validColumnsAfterRewrite :+ provenanceCollection
    var groupingExpressionsAfterExpand = SchemaAlternativesExpressionAlternatives(provenanceContext.primarySchemaAlternative, expand, aggregate.groupingExpressions).forwardTraceNamedExpressions()
    groupingExpressionsAfterExpand = groupingExpressionsAfterExpand ++ groupingColumn
    val rewrittenAggregate = Aggregate(groupingExpressionsAfterExpand, extendedAggregateExpressionAfterExpand, expand)



    val updatedTree = SchemaAlternativesForwardTracing(provenanceContext.primarySchemaAlternative, rewrittenAggregate, aggregate.aggregateExpressions).forwardTraceExpressions().forwardTraceConstraintsOnAggregatedValues(outputWhyNotQuestion).getOutputWhyNotQuestion()
    val aggregatedProvenanceAttribute = ProvenanceAttribute(oid, Constants.getProvenanceCollectionFieldName(oid), provenanceCollection.dataType)
    val aggregateContext = ProvenanceContext(provenanceContext, aggregatedProvenanceAttribute)
    aggregateContext.primarySchemaAlternative = updatedTree
    aggregateContext.replaceValidAttributes(provenanceContext.getValidAttributes())

    val newCompatibleColumns = compatibleColumns(rewrittenAggregate, aggregateContext)
    val newValidColumns = getValidColumns(rewrittenAggregate, aggregateContext, validNames)
    val finalOutput = mutable.ListBuffer[NamedExpression](rewrittenAggregate.output: _*)
    finalOutput --= getAttributesByName(rewrittenAggregate.output, validNames)
    finalOutput --= groupingColumn
    finalOutput ++= newCompatibleColumns
    finalOutput ++= newValidColumns
    val rewrittenExpression = Project(finalOutput.toList, rewrittenAggregate)

    Rewrite(rewrittenExpression, aggregateContext)
  }
}


