package de.uni_stuttgart.ipvs.provenance.transformations
//import com.sun.tools.corba.se.idl.constExpr.NotEqual
import de.uni_stuttgart.ipvs.provenance.nested_why_not.{Constants, ProvenanceAttribute, ProvenanceContext, Rewrite, WhyNotPlanRewriter}
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.{AlternativeOidAdder, PrimarySchemaSubsetTree, SchemaAlternativesExpressionAlternatives, SchemaAlternativesForwardTracing, SchemaSubsetTree, SchemaSubsetTreeAccessAdder, SchemaSubsetTreeBackTracing}
import de.uni_stuttgart.ipvs.provenance.why_not_question.SchemaBackTrace
import org.apache.spark.sql.catalyst.expressions.{Add, Alias, And, Attribute, AttributeReference, CaseWhen, CreateNamedStruct, CreateStruct, EqualTo, Expression, IsNull, Literal, NamedExpression, Not}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, CollectList, Complete, Count, First, Max, Min}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Expand, GroupingSets, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.{analysis, expressions}
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

  def getOriginalAggregations(plan: LogicalPlan, provenanceContext: ProvenanceContext): Seq[NamedExpression] = {
    val originalColumns = provenanceContext.getExpressionsFromProvenanceAttributes(provenanceContext.getOriginalAttributes(), plan.output)
    val aggregateExpressions = mutable.ListBuffer.empty[NamedExpression]
    for (validColumn <- originalColumns){
      val expression = getOriginalAggregation(validColumn)
      aggregateExpressions += expression
    }
    aggregateExpressions.toList
  }

  def getValidAggregation(attribute: NamedExpression): (NamedExpression, String) = {
    val oldName = Constants.getValidFieldWithOldExtension(attribute.name)
    getProvenanceAggregation(attribute, oldName)
  }

  def getProvenanceAggregation(attribute: NamedExpression, name: String) : (NamedExpression, String) = {
    val expression = Alias(
      AggregateExpression(
        Max(attribute),
        Complete, false),
      name)()
    (expression, name)
  }

  def getFirstAggregation(attribute: NamedExpression) : NamedExpression = {
    val expression = Alias(
      AggregateExpression(
        First(attribute, Literal(true, BooleanType)),
        Complete, false),
      attribute.name)()
    expression
  }

  def getOriginalAggregation(attribute: NamedExpression): (NamedExpression) = {
    val oldName = attribute.name
    getProvenanceAggregation(attribute, oldName)._1
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

  def validCondition(alternativeExpression: NamedExpression, validColumn: NamedExpression): (Expression, Expression) = {
    val condition = EqualTo(validColumn, Literal(true, BooleanType))
    val trueCase = alternativeExpression
    (condition, trueCase)
  }

  def createFilledGroupingAttribute(alternativeGroupingExpressions: Seq[NamedExpression], validColumns: Seq[NamedExpression], targetName: String): NamedExpression = {
    val branches = (alternativeGroupingExpressions zip validColumns).map {
      case (alternativeColumn, validColumn) => validCondition(alternativeColumn, validColumn)
    }
    val elseValue: Option[Expression] = Some(Literal(null, alternativeGroupingExpressions(0).dataType))
    Alias(CaseWhen(branches, elseValue), targetName)()
  }

  def nullInvalidValues(targetColumns: Seq[NamedExpression], validColumns: Seq[NamedExpression]): Seq[NamedExpression] = {
    (targetColumns zip validColumns).map {
      case (alternativeColumn, validColumn) => Alias(nullInvalidValues(alternativeColumn, validColumn), alternativeColumn.name)()
    }
  }

  def nullInvalidValues(targetColumn: NamedExpression, validColumn: NamedExpression, name: Option[String] = None): NamedExpression = {
    val newName = if(name.isDefined) name.get else targetColumn.name
    val condition = EqualTo(validColumn, Literal(true, BooleanType))
    val trueCase = targetColumn
    val falseCase = Some(Literal(null, targetColumn.dataType))
    val branches = Seq(Tuple2(condition, trueCase))
    Alias(CaseWhen(branches, falseCase), newName)()
  }

  /*
  There exist two problems with the grouping:
  1) When attributes occur multiple times in the grouping set, the plan will have them multiple times in the result with the same name, which causes ambiguity problems
  2) When the grouping attributes are the same, the query still creates a grouping set for them (obvious optimization)
   */

  /*
  Two things need to be done to fix the problem:
  a) grouping attributes need to be renamed
  b) provenance trees need an update
   */

  def getOptimizedProjections(context: ProvenanceContext, provenanceProjection: Project, provenanceColumns: Seq[Attribute]): (Seq[Seq[Expression]], Seq[Attribute]) = {
    var groupingExpressions = SchemaAlternativesExpressionAlternatives(context.primarySchemaAlternative, provenanceProjection, aggregate.groupingExpressions).forwardTraceNamedExpressions()
    var aggregateExpressionWithoutAggregate = SchemaAlternativesExpressionAlternatives(context.primarySchemaAlternative, provenanceProjection, aggregate.aggregateExpressions).isForGroupingSets().forwardTraceNamedExpressions()
    val distinctAggregateExpressionWithoutAggregate = aggregateExpressionWithoutAggregate.distinct
    val distinctGroupingExpressions = groupingExpressions.distinct

    val groupedAggregateExpressions = mutable.ListBuffer.empty[Seq[Expression]]
    for ((alternative, altIdx) <- context.primarySchemaAlternative.getAllAlternatives().zipWithIndex){

      val outputAggregateAttributes = mutable.ListBuffer.empty[Expression]
      val relevantAttributes = mutable.ListBuffer.empty[NamedExpression]

      for(idx <- altIdx until aggregateExpressionWithoutAggregate.size by (context.primarySchemaAlternative.alternatives.size + 1)){
        relevantAttributes += aggregateExpressionWithoutAggregate(idx)
      }
      for(attribute <- distinctAggregateExpressionWithoutAggregate){
        if (relevantAttributes.contains(attribute) || !distinctGroupingExpressions.contains(attribute)){
          outputAggregateAttributes += attribute
        } else {
          outputAggregateAttributes += Literal(null, attribute.dataType)
        }
      }
      outputAggregateAttributes ++= provenanceColumns
      outputAggregateAttributes += Literal(alternative.id, IntegerType)
      groupedAggregateExpressions += outputAggregateAttributes.toList
    }

    val output = mutable.ListBuffer.empty[Attribute]
    output ++= distinctAggregateExpressionWithoutAggregate.map {
      case groupingAttribute:Attribute if (distinctGroupingExpressions.contains(groupingAttribute)) => {groupingAttribute.newInstance()}
      case aggregationAttribute:Attribute if (distinctAggregateExpressionWithoutAggregate.contains(aggregationAttribute)) => {aggregationAttribute}
    }
    output ++= provenanceColumns
    output += AttributeReference(getExpandAttributeName, IntegerType, false)()
    (groupedAggregateExpressions.toList, output.toList)
  }

  def getProjections(context: ProvenanceContext, aggregateExpressions: Seq[NamedExpression], groupingExpressions: Seq[NamedExpression]): (Seq[Seq[Expression]], Seq[Attribute]) = {

    val extendedAggregateExpressions = aggregateExpressions :+ AttributeReference(getExpandAttributeName, IntegerType, false)()
    val groupedAggregateExpressions = mutable.ListBuffer.empty[Seq[Expression]]
    val moduloFactor = context.primarySchemaAlternative.getAllAlternatives().size
    for ((alternative, altIdx) <- context.primarySchemaAlternative.getAllAlternatives().zipWithIndex){
      val outputAggregateAttributes = mutable.ListBuffer.empty[Expression]
      for(idx <- 0 until groupingExpressions.size){
        if (idx % moduloFactor == altIdx){
          outputAggregateAttributes += extendedAggregateExpressions(idx)
        } else {
          outputAggregateAttributes += Literal(null, extendedAggregateExpressions(idx).dataType)
        }
      }
      for(idx <- groupingExpressions.size until extendedAggregateExpressions.size - 1) {
        outputAggregateAttributes += extendedAggregateExpressions(idx)
      }
      outputAggregateAttributes += Literal(alternative.id, IntegerType)
      groupedAggregateExpressions += outputAggregateAttributes.toList
    }
    val newGroupingExpressions = groupingExpressions.map{_.newInstance()}
    val output = mutable.ListBuffer.empty[NamedExpression]
    output ++= newGroupingExpressions
    output ++= extendedAggregateExpressions.map{expr => expr.asInstanceOf[Attribute]}
    output --= groupingExpressions
    (groupedAggregateExpressions.distinct.toList, output.distinct.map{expr => expr.asInstanceOf[Attribute]})
  }

  def getAlternativeDependentProvenanceCollection(provenanceContext: ProvenanceContext, provenanceCollection: NamedExpression, validColumns: Seq[NamedExpression]) : Seq[NamedExpression] = {
    val nestedCollectionProvenanceAttributes = mutable.ListBuffer.empty[ProvenanceAttribute]
    val provenanceCollectionColumns = mutable.ListBuffer.empty[NamedExpression]
    for ((validColumn, alternative) <- validColumns zip provenanceContext.primarySchemaAlternative.getAllAlternatives()) {
      val name = Constants.getProvenanceCollectionFieldName(oid, alternative.id)
      provenanceCollectionColumns += nullInvalidValues(provenanceCollection, validColumn, Some(name))
      nestedCollectionProvenanceAttributes += ProvenanceAttribute(oid, name, provenanceCollection.dataType)
    }
    val oldAttribute = provenanceContext.provenanceAttributes.filter(p => p.attributeName == provenanceCollection.name).head
    provenanceContext.replaceSingleNestedProvenanceContextWithSchemaAlternativeContexts(oldAttribute, nestedCollectionProvenanceAttributes)
    provenanceCollectionColumns
  }

  override def rewriteWithAlternatives(): Rewrite = {

    val childRewrite = child.rewriteWithAlternatives()
    val rewrittenChild = childRewrite.plan
    val provenanceContext = childRewrite.provenanceContext


    val provenanceTuple = getProvenanceTuple(childRewrite)
    val provenanceProjection = Project(rewrittenChild.output :+ provenanceTuple , rewrittenChild)
    val provenanceLegacy = getAttributeByName(provenanceProjection.output, provenanceTuple.name)

    val validColumns = provenanceContext.getExpressionsFromProvenanceAttributes(provenanceContext.getValidAttributes(), provenanceProjection.output)
    val originalColumns = provenanceContext.getExpressionsFromProvenanceAttributes(provenanceContext.getOriginalAttributes(), provenanceProjection.output)

    val provenanceColumns = validColumns ++ originalColumns ++ provenanceLegacy

    val groupingExpressions = SchemaAlternativesExpressionAlternatives(provenanceContext.primarySchemaAlternative, provenanceProjection, aggregate.groupingExpressions).forwardTraceNamedExpressions()

    var aggregateExpressionWithoutAggregate = SchemaAlternativesExpressionAlternatives(provenanceContext.primarySchemaAlternative, provenanceProjection, aggregate.aggregateExpressions).isForGroupingSets().forwardTraceNamedExpressions()
    aggregateExpressionWithoutAggregate = aggregateExpressionWithoutAggregate ++ provenanceColumns



    //val (projections, output) = getProjections(provenanceContext, aggregateExpressionWithoutAggregate, groupingExpressions)
    val (projections, output) = getOptimizedProjections(provenanceContext, provenanceProjection,provenanceColumns.map {case (a: Attribute) => a} )

    //create a group for each schema alternative
    val expand = Expand(projections, output, provenanceProjection)


    val provenanceCollection = getNestedProvenanceCollection(expand)
    var aggregateExpressionAfterExpand = SchemaAlternativesExpressionAlternatives(provenanceContext.primarySchemaAlternative, expand, aggregate.aggregateExpressions).forwardTraceNamedExpressions().distinct//.filterNot(ne => ne.name == "sum")
    val (validColumnsAfterRewrite, validNames) = getValidAggregations(expand, provenanceContext)
    val originalColumnsAfterRewrite = getOriginalAggregations(expand, provenanceContext)

    val groupingColumn = getAttributeByName(expand.output, getExpandAttributeName)
    val extendedAggregateExpressionAfterExpand = aggregateExpressionAfterExpand ++ groupingColumn ++ validColumnsAfterRewrite ++ originalColumnsAfterRewrite :+ provenanceCollection
    val groupingExpressionsAfterExpand = SchemaAlternativesExpressionAlternatives(provenanceContext.primarySchemaAlternative, expand, aggregate.groupingExpressions).forwardTraceNamedExpressions().distinct
    val groupingExpressionsAfterExpandWithGroupingColumn = groupingExpressionsAfterExpand ++ groupingColumn
    //aggregate values for each group
    val rewrittenAggregate = Aggregate(groupingExpressionsAfterExpandWithGroupingColumn, extendedAggregateExpressionAfterExpand, expand)



    AlternativeOidAdder(provenanceContext, aggregate.aggregateExpressions, oid).traceAttributeAccess()
    val updatedTree = SchemaAlternativesForwardTracing(provenanceContext.primarySchemaAlternative, rewrittenAggregate, aggregate.aggregateExpressions).forwardTraceExpressions().forwardTraceConstraintsOnAggregatedValues(outputWhyNotQuestion).getOutputWhyNotQuestion()
    val aggregatedProvenanceAttribute = ProvenanceAttribute(oid, Constants.getProvenanceCollectionFieldName(oid), provenanceCollection.dataType)
    val aggregateContext = ProvenanceContext(provenanceContext, aggregatedProvenanceAttribute)
    aggregateContext.primarySchemaAlternative = updatedTree
    aggregateContext.replaceValidAttributes(provenanceContext.getValidAttributes())
    aggregateContext.replaceOriginalAttributes(provenanceContext.getOriginalAttributes())


    val newValidColumns = getValidColumns(rewrittenAggregate, aggregateContext, validNames)
    val intermediateOutput = mutable.ListBuffer[NamedExpression](rewrittenAggregate.output: _*)
    intermediateOutput --= getAttributesByName(rewrittenAggregate.output, validNames)
    intermediateOutput --= groupingColumn
    intermediateOutput ++= newValidColumns
    //TODO multiply provenanceCollection
    //Add new valid columns
    val intermediateProjection = Project(intermediateOutput.toList, rewrittenAggregate)

    val intermediateValidColumns : Seq[NamedExpression] = aggregateContext.getExpressionsFromProvenanceAttributes(provenanceContext.getValidAttributes(), intermediateProjection.output)
    val provenanceAttributeAggregateNames = mutable.ListBuffer.empty[String]
    val secondGroupingExpressions = mutable.ListBuffer.empty[NamedExpression]
    // assumes distinct grouping attributes, otherwise it does not work correctly,
    // in the initial implementation, the grouping attributes are distinct for each alternative,
    // Each genuine attribute is followed by all alternatives, before the next attribute
    //for(alternatives <- groupingExpressionsAfterExpand.grouped(provenanceContext.primarySchemaAlternative.getAllAlternatives().size)){
    for(groupingAttribute <- aggregate.groupingExpressions){
      val groupingAttributeName = groupingAttribute match {
        case attribute : NamedExpression => attribute.name
      }
      val aggregateAttributeName = Constants.getGroupingFieldNameWithAggregatePostfix(groupingAttributeName)
      provenanceAttributeAggregateNames += aggregateAttributeName
      //secondGroupingExpressions += createFilledGroupingAttribute(alternatives, intermediateValidColumns, aggregateAttributeName)
      val attributeSeq = getAttributeByName(intermediateProjection.output, groupingAttributeName).toSeq
      val alternatives = SchemaAlternativesExpressionAlternatives(provenanceContext.primarySchemaAlternative, intermediateProjection, attributeSeq).forwardTraceNamedExpressions() //must not be distinct
      secondGroupingExpressions +=createFilledGroupingAttribute(alternatives, intermediateValidColumns, aggregateAttributeName)
    }

    val initialAggregateNames = mutable.ListBuffer[NamedExpression](aggregate.aggregateExpressions: _*)
    initialAggregateNames --= aggregate.groupingExpressions.map {
      case expr: NamedExpression => expr
    }
    val aggregateNames = initialAggregateNames.map {expr =>expr.name}
    val aggregateManipulationExpressions = getAttributesByName( intermediateProjection.output, aggregateNames.toList)
    val resultAggregateExpressions = SchemaAlternativesExpressionAlternatives(aggregateContext.primarySchemaAlternative, intermediateProjection, aggregateManipulationExpressions).forwardTraceNamedExpressions()
    val secondAggregateExpressions = mutable.ListBuffer.empty[NamedExpression]

    //fix needed here, as well?
    for(alternatives <- resultAggregateExpressions.grouped(aggregateContext.primarySchemaAlternative.getAllAlternatives().size)){
      secondAggregateExpressions ++= nullInvalidValues(alternatives, intermediateValidColumns)
    }
    val originalAttributes = aggregateContext.getExpressionsFromProvenanceAttributes(aggregateContext.getOriginalAttributes(), intermediateProjection.output)
    secondAggregateExpressions ++= nullInvalidValues(originalAttributes, intermediateValidColumns)

    val nestedProvenanceContextAttribute: ProvenanceAttribute = aggregateContext.getNestedProvenanceAttributes().filter(p => p._1.attributeName == Constants.getProvenanceCollectionFieldName(oid)).head._1
    val nestedProvenanceColumn = aggregateContext.getExpressionFromProvenanceAttribute(nestedProvenanceContextAttribute, intermediateProjection.output).get
    secondAggregateExpressions ++= getAlternativeDependentProvenanceCollection(aggregateContext, nestedProvenanceColumn, intermediateValidColumns)




    val finalOutput = mutable.ListBuffer[NamedExpression](intermediateProjection.output: _*)
    finalOutput ++= secondGroupingExpressions
    finalOutput --= resultAggregateExpressions
    finalOutput --= originalAttributes
    finalOutput -= nestedProvenanceColumn
    finalOutput ++= secondAggregateExpressions
    // null all values for invalid fields
    val rewrittenExpression = Project(finalOutput.toList, intermediateProjection)


    val secondAggregateExpressionsAfterProjection: Seq[NamedExpression] = getAttributesByName(rewrittenExpression.output, secondAggregateExpressions.map(expr => expr.name)).map(getFirstAggregation)
    val secondValidFields: Seq[NamedExpression] = intermediateValidColumns.map(attr => getProvenanceAggregation(attr, attr.name)._1)
    val formerGroupingAttributes = groupingExpressionsAfterExpand.map(getFirstAggregation)
    val secondGroupExpressionsAfterProjection = getAttributesByName(rewrittenExpression.output, secondGroupingExpressions.map(expr => expr.name))

    //group by old values, pick first non-null value
    val secondAggregation = Aggregate(secondGroupExpressionsAfterProjection, formerGroupingAttributes ++ secondAggregateExpressionsAfterProjection ++ secondValidFields, rewrittenExpression)


    val newCompatibleColumns = compatibleColumns(secondAggregation, aggregateContext)
    //add new compatible field
    val finalProjection = Project(secondAggregation.output ++ newCompatibleColumns, secondAggregation)


    Rewrite(finalProjection, aggregateContext)
  }
}


