package de.uni_stuttgart.ipvs.provenance.nested_why_not

import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.DataType

import scala.collection.mutable

case class ProvenanceAttribute(oid: Int, attributeName: String, attributeType: DataType)

object ProvenanceContext {

  protected[provenance] def mergeContext(leftContext: ProvenanceContext, rightContext: ProvenanceContext): ProvenanceContext = {
    val provenanceContext = new ProvenanceContext()
    provenanceContext.nestedProvenanceContexts ++= leftContext.nestedProvenanceContexts
    provenanceContext.nestedProvenanceContexts ++= rightContext.nestedProvenanceContexts
    provenanceContext.provenanceAttributes ++= leftContext.provenanceAttributes
    provenanceContext.provenanceAttributes ++= rightContext.provenanceAttributes
    provenanceContext
  }

  def apply() = new ProvenanceContext()

  def apply(childContext: ProvenanceContext, provenanceAttribute: ProvenanceAttribute) = {
    val provenanceContext = new ProvenanceContext()
    provenanceContext.addNestedProvenanceContext(childContext)
    provenanceContext.addProvenanceAttribute(provenanceAttribute)
    provenanceContext
  }

}

class ProvenanceContext {


  //TODO also associate with nodes in the schema subset tree aka. schema alternatives
  protected[provenance] val nestedProvenanceContexts = mutable.ListBuffer.empty[ProvenanceContext]

  protected[provenance] val provenanceAttributes = mutable.ListBuffer.empty[ProvenanceAttribute]

  protected[provenance] var mostRecentCompatibleAttribute: ProvenanceAttribute = null
  protected[provenance] var mostRecentSurvivorAttribute: ProvenanceAttribute = null
  protected[provenance] var validAttribute: ProvenanceAttribute = null

  protected def addNestedProvenanceContext(provenanceContext: ProvenanceContext): Unit = {
    nestedProvenanceContexts += provenanceContext
  }


  protected def addProvenanceAttribute(provenanceAttribute: ProvenanceAttribute): Unit = {
    provenanceAttributes += provenanceAttribute
  }

  protected[provenance] def addCompatibilityAttribute(compatibilityAttribute: ProvenanceAttribute): Unit = {
    addProvenanceAttribute(compatibilityAttribute)
    mostRecentCompatibleAttribute = compatibilityAttribute
  }

  protected[provenance] def addSurvivorAttribute(survivorAttribute: ProvenanceAttribute): Unit = {
    addProvenanceAttribute(survivorAttribute)
    mostRecentSurvivorAttribute = survivorAttribute
  }

  protected[provenance] def getCompatibilityAttribute(oid: Int): Option[ProvenanceAttribute] = {
    provenanceAttributes.find(a => {a.oid == oid && a.attributeName.contains(Constants.COMPATIBLE_FIELD)})
  }

  protected[provenance] def getMostRecentCompatibilityAttribute():Option[ProvenanceAttribute] = {
    if (mostRecentCompatibleAttribute == null) return None
    Some(mostRecentCompatibleAttribute)
  }

  protected[provenance] def getSurvivedFieldAttribute(oid: Int): Option[ProvenanceAttribute] = {
    provenanceAttributes.find(a => {a.oid == oid && a.attributeName.contains(Constants.SURVIVED_FIELD)})
  }

  protected[provenance] def getMostRecentSurvivedAttribute():Option[ProvenanceAttribute] = {
    if (mostRecentSurvivorAttribute == null) return None
    Some(mostRecentSurvivorAttribute)
  }

  protected[provenance] def getValidAttribute(): Option[ProvenanceAttribute] = {
    provenanceAttributes.find(a => a.attributeName.contains(Constants.VALID_FIELD))
  }

  protected[provenance] def getExpressionFromProvenanceAttribute(attribute: ProvenanceAttribute, expressions: Seq[NamedExpression]): Option[NamedExpression] = {
    expressions.find(ex => ex.name == attribute.attributeName)
  }

  protected[provenance] def getExpressionFromAllProvenanceAttributes(expressions: Seq[NamedExpression]): Seq[NamedExpression] = {
    provenanceAttributes.foldLeft(List.empty[NamedExpression])
    {(provenanceExpressions, attribute) => provenanceExpressions ++ getExpressionFromProvenanceAttribute(attribute, expressions)}
  }









}
