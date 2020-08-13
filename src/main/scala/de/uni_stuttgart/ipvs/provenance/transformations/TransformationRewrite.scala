package de.uni_stuttgart.ipvs.provenance.transformations

import de.uni_stuttgart.ipvs.provenance.nested_why_not.{Constants, ProvenanceAttribute, ProvenanceContext, Rewrite}
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.SchemaSubsetTree
import org.apache.spark.sql.catalyst.expressions.{Alias, CreateStruct, Expression, NamedExpression, ScalaUDF}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.{BooleanType, DataType}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

trait TransformationRewrite {

  def plan: LogicalPlan
  var whyNotQuestion: SchemaSubsetTree = SchemaSubsetTree()
  def oid: Int

  def children: Seq[TransformationRewrite]

  def backtraceWhyNotQuestion(whyNotQuestion: SchemaSubsetTree): Unit = {
    this.whyNotQuestion = whyNotQuestion
    backtraceChildrenWhyNotQuestion
  }

  protected def backtraceChildrenWhyNotQuestion : Unit

  def compatibleColumn(child: LogicalPlan, provenanceContext: ProvenanceContext): NamedExpression = {
    val udfExpression = getDataFetcherExpression(child)
    val attributeName = Constants.getCompatibleFieldName(oid)
    provenanceContext.addCompatibilityAttribute(ProvenanceAttribute(oid, attributeName, BooleanType))
    Alias(udfExpression,attributeName)()
  }

  def compatibleColumn(child: LogicalPlan, provenanceContext: ProvenanceContext, schemaAlternative: SchemaSubsetTree): (NamedExpression, ProvenanceAttribute) = {
    val udfExpression = getDataFetcherExpression(child, schemaAlternative)
    val attributeName = Constants.getCompatibleFieldName(oid, schemaAlternative.id)
    (Alias(udfExpression,attributeName)(), ProvenanceAttribute(oid, attributeName, BooleanType))
  }

  def compatibleColumns(child: LogicalPlan, provenanceContext: ProvenanceContext): Seq[NamedExpression] = {
    val compatibleExpressions = mutable.ListBuffer.empty[NamedExpression]
    val compatibleAttributes = mutable.ListBuffer.empty[ProvenanceAttribute]
    for (alternative <- provenanceContext.primarySchemaAlternative.getAllAlternatives()){
      val (expression, provenanceAttribute) = compatibleColumn(child, provenanceContext, alternative)
      compatibleExpressions += expression
      compatibleAttributes += provenanceAttribute
    }
    provenanceContext.addCompatibilityAttributes(compatibleAttributes.toList)
    compatibleExpressions.toList
  }





  def getDataFetcherExpression(child: LogicalPlan, schemaAlternative: SchemaSubsetTree = whyNotQuestion) = {
    val udf = ProvenanceContext.getUDF
    val children = ArrayBuffer[Expression](getNamedStructExpression(child.output), schemaAlternative.getSchemaSubsetTreeExpression)
    val inputIsNullSafe = true :: true :: Nil
    val inputTypes = udf.inputTypes.getOrElse(Seq.empty[DataType])
    val udfName = Some(Constants.getUDFName)
    ScalaUDF(udf.f, udf.dataType, children, inputIsNullSafe, inputTypes, udfName)
  }

  def getNamedStructExpression(output: Seq[Expression]): Expression = {
    CreateStruct(output)
  }


  def addCompatibleAttributeToProvenanceContext(provenanceContext: ProvenanceContext) = {
    val attributeName = Constants.getCompatibleFieldName(oid)
    provenanceContext.addCompatibilityAttribute(ProvenanceAttribute(oid, attributeName, BooleanType))
    attributeName
  }

  def addCompatibleAttributeToProvenanceContext(provenanceContext: ProvenanceContext, alternativeIdx: Int) = {
    val attributeName = Constants.getCompatibleFieldName(oid, alternativeIdx)
    provenanceContext.addCompatibilityAttribute(ProvenanceAttribute(oid, attributeName, BooleanType))
    attributeName
  }

  def getPreviousCompatible(rewrite: Rewrite): NamedExpression = {
    val attribute = rewrite.provenanceContext.getMostRecentCompatibilityAttribute()
      .getOrElse(throw new MatchError("Unable to find previous compatible structure in provenance structure"))
    getPreviousCompatible(rewrite.plan, attribute)
  }

  def getPreviousCompatible(rewrite: Rewrite, alternativeIdx: Int): NamedExpression = {
    val attribute = rewrite.provenanceContext.getMostRecentCompatibilityAttribute(alternativeIdx)
      .getOrElse(throw new MatchError("Unable to find previous compatible structure in provenance structure"))
    getPreviousCompatible(rewrite.plan, attribute)
  }

  def getPreviousCompatibles(rewrite: Rewrite): Seq[NamedExpression] = {
    val attributes = rewrite.provenanceContext.getMostRecentCompatibilityAttributes()
    attributes.map(attribute => getPreviousCompatible(rewrite.plan, attribute))
  }

  def getPreviousCompatible(rewrittenPlan: LogicalPlan, attribute: ProvenanceAttribute): NamedExpression = {
    val compatibleAttribute = rewrittenPlan.output.find(ex => ex.name == attribute.attributeName)
      .getOrElse(throw new MatchError("Unable to find previous compatible structure in output of previous operator"))
    compatibleAttribute
  }



  def rewrite():Rewrite

  def rewriteWithAlternatives():Rewrite

}
