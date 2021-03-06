package de.uni_stuttgart.ipvs.provenance.nested_why_not

import de.uni_stuttgart.ipvs.provenance.why_not_question.DataFetcherUDF
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.expressions.UserDefinedFunction
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
    provenanceContext.addNestedProvenanceContext(childContext, provenanceAttribute)
    provenanceContext
  }

  protected var udf : UserDefinedFunction = null

  protected[provenance] def initializeUDF(dataFrame: DataFrame) = {
    udf = dataFrame.sparkSession.udf.register(Constants.getUDFName, new DataFetcherUDF().call _)
  }

  def getUDF = udf

}

class ProvenanceContext {


  //TODO also associate with nodes in the schema subset tree aka. schema alternatives
  protected[provenance] val nestedProvenanceContexts = mutable.Map.empty[ProvenanceAttribute, ProvenanceContext]

  protected[provenance] val provenanceAttributes = mutable.ListBuffer.empty[ProvenanceAttribute]

  protected[provenance] var mostRecentCompatibleAttribute: ProvenanceAttribute = null
  protected[provenance] var mostRecentSurvivorAttribute: ProvenanceAttribute = null
  protected[provenance] var validAttribute: ProvenanceAttribute = null

  protected def addNestedProvenanceContext(provenanceContext: ProvenanceContext, provenanceAttribute: ProvenanceAttribute): Unit = {
    nestedProvenanceContexts.put(provenanceAttribute, provenanceContext)
    addProvenanceAttribute(provenanceAttribute)
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

  protected[provenance] def addIDAttribute(idAttribute: ProvenanceAttribute): Unit = {
    addProvenanceAttribute(idAttribute)
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

  protected[provenance] def getNestedProvenanceAttributes(): Seq[(ProvenanceAttribute, ProvenanceContext)] = {
    nestedProvenanceContexts.toSeq
  }

  protected[provenance] def isNestedProvenanceAttribute(provenanceAttribute: ProvenanceAttribute): Boolean = {
    nestedProvenanceContexts.contains(provenanceAttribute)
  }

  protected[provenance] def isMostRecentCompatibleAttribute(provenanceAttribute: ProvenanceAttribute): Boolean = {
    provenanceAttribute == mostRecentCompatibleAttribute
  }

  protected[provenance] def isSurvivedAttribute(provenanceAttribute: ProvenanceAttribute): Boolean = {
    Constants.isSurvivedField(provenanceAttribute.attributeName)
  }

  protected[provenance] def isIDAttribute(provenanceAttribute: ProvenanceAttribute): Boolean = {
    Constants.isIDField(provenanceAttribute.attributeName)
  }


  protected[provenance] def isProvenanceAttribute(expression: NamedExpression): Boolean = {
    //TODO if called from a list of size m, this call yields O(m*n) complexity
    provenanceAttributes.exists( attribute =>
      attribute.attributeName == expression.name
    )
  }

  protected[provenance] def getProvenanceAttributes(): Seq[ProvenanceAttribute] = {
    provenanceAttributes
  }

}
