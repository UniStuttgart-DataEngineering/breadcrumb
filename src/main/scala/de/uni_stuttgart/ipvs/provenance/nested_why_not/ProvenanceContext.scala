package de.uni_stuttgart.ipvs.provenance.nested_why_not

import de.uni_stuttgart.ipvs.provenance.evaluation.TestScenario
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.{FlattenIndexUDF, PrimarySchemaSubsetTree}
import de.uni_stuttgart.ipvs.provenance.why_not_question.{DataFetcher, DataFetcherUDF}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.DataType

import scala.collection.mutable

case class ProvenanceAttribute(oid: Int, attributeName: String, attributeType: DataType)

object ProvenanceContext {

  def associateIds(provenanceContext: ProvenanceContext, leftContext: ProvenanceContext, rightContext: ProvenanceContext): Unit = {
    val totalNumberOfAlternatives = leftContext.primarySchemaAlternative.getAllAlternatives().size * rightContext.primarySchemaAlternative.getAllAlternatives().size
    val alternatingFactor = rightContext.primarySchemaAlternative.getAllAlternatives().size
    for (idx <- 0 until totalNumberOfAlternatives){
      val outputId = provenanceContext.primarySchemaAlternative.getAllAlternatives()(idx).id
      val leftId = leftContext.primarySchemaAlternative.getAllAlternatives()((idx) / alternatingFactor).id
      val rightId = rightContext.primarySchemaAlternative.getAllAlternatives()((idx) % alternatingFactor).id
      provenanceContext.associatedIds.put(outputId, (leftId, rightId))
    }
  }

  protected[provenance] def mergeContext(leftContext: ProvenanceContext, rightContext: ProvenanceContext): ProvenanceContext = {
    val provenanceContext = new ProvenanceContext()
    provenanceContext.nestedProvenanceContexts ++= leftContext.nestedProvenanceContexts
    provenanceContext.nestedProvenanceContexts ++= rightContext.nestedProvenanceContexts
    provenanceContext.provenanceAttributes ++= leftContext.provenanceAttributes
    provenanceContext.provenanceAttributes ++= rightContext.provenanceAttributes
    provenanceContext.provenanceAttributes --= leftContext.getValidAttributes()
    provenanceContext.provenanceAttributes --= rightContext.getValidAttributes()
    provenanceContext.provenanceAttributes --= leftContext.getOriginalAttributes()
    provenanceContext.provenanceAttributes --= rightContext.getOriginalAttributes()
    provenanceContext.primarySchemaAlternative = mergeSchemaAlternatives(leftContext, rightContext)
    provenanceContext.associatedIds ++= leftContext.associatedIds
    provenanceContext.associatedIds ++= rightContext.associatedIds
    associateIds(provenanceContext, leftContext, rightContext)
    provenanceContext.operatorsModifiedByAlternatives ++= leftContext.operatorsModifiedByAlternatives
    provenanceContext.operatorsModifiedByAlternatives ++= rightContext.operatorsModifiedByAlternatives
    provenanceContext
  }

  protected[provenance] def mergeSchemaAlternatives(leftContext: ProvenanceContext, rightContext: ProvenanceContext): PrimarySchemaSubsetTree = {
    (leftContext.primarySchemaAlternative, rightContext.primarySchemaAlternative) match {
      case (null, null) => {
        null : PrimarySchemaSubsetTree
      }
      case (left, right) => {
        PrimarySchemaSubsetTree.merge(left, right)
      }
    }


  }

  def apply() = new ProvenanceContext()

  def apply(childContext: ProvenanceContext, provenanceAttribute: ProvenanceAttribute) = {
    val provenanceContext = new ProvenanceContext()
    provenanceContext.addNestedProvenanceContext(childContext, provenanceAttribute)
    provenanceContext.associatedIds ++= childContext.associatedIds
    provenanceContext.operatorsModifiedByAlternatives ++= childContext.operatorsModifiedByAlternatives
    provenanceContext
  }

  protected var dataFetcherUdf : UserDefinedFunction = null
  protected var flattenUdf : UserDefinedFunction = null

  protected[provenance] def initializeUDF(dataFrame: DataFrame) = {
    dataFetcherUdf = dataFrame.sparkSession.udf.register(Constants.getDataFetcherUDFName(), new DataFetcherUDF().call _)
    flattenUdf = dataFrame.sparkSession.udf.register(Constants.getFlattenUDFName(), new FlattenIndexUDF().call _)
  }

  def getDataFetcherUDF = dataFetcherUdf
  def getFlattenUDF = flattenUdf


  var testScenario: TestScenario = null

  def setTestScenario(testScenario: TestScenario) = {
    this.testScenario = testScenario
  }

}

class ProvenanceContext {


  //TODO also associate with nodes in the schema subset tree aka. schema alternatives
  protected[provenance] var primarySchemaAlternative: PrimarySchemaSubsetTree = null

  protected[provenance] val nestedProvenanceContexts = mutable.Map.empty[ProvenanceAttribute, ProvenanceContext]

  protected[provenance] val provenanceAttributes = mutable.ListBuffer.empty[ProvenanceAttribute]

  protected[provenance] var mostRecentCompatibleAttribute: ProvenanceAttribute = null
  protected[provenance] var mostRecentSurvivorAttribute: ProvenanceAttribute = null

  protected[provenance] var mostRecentCompatibleAttributes: Seq[ProvenanceAttribute] = List.empty[ProvenanceAttribute]
  protected[provenance] var mostRecentSurvivorAttributes: Seq[ProvenanceAttribute] = List.empty[ProvenanceAttribute]
  protected[provenance] var validAttributes: Seq[ProvenanceAttribute] = List.empty[ProvenanceAttribute]
  protected[provenance] var originalAttributes: Seq[ProvenanceAttribute] = List.empty[ProvenanceAttribute]

  protected[provenance] val associatedIds = mutable.Map.empty[Int, (Int, Int)]

  protected[provenance] val operatorsModifiedByAlternatives = mutable.Map.empty[Int, mutable.Set[Int]]


  protected def addNestedProvenanceContext(provenanceContext: ProvenanceContext, provenanceAttribute: ProvenanceAttribute): Unit = {
    nestedProvenanceContexts.put(provenanceAttribute, provenanceContext)
    addProvenanceAttribute(provenanceAttribute)
  }

  protected[provenance] def replaceSingleNestedProvenanceContextWithSchemaAlternativeContexts(oldAttribute: ProvenanceAttribute, newAttributes: Seq[ProvenanceAttribute]) = {
    provenanceAttributes -= oldAttribute
    val nestedContext = nestedProvenanceContexts.get(oldAttribute).get
    newAttributes.map (attr => nestedProvenanceContexts.put(attr, nestedContext))
    addProvenanceAttributes(newAttributes)
  }


  protected def addProvenanceAttribute(provenanceAttribute: ProvenanceAttribute): Unit = {
    provenanceAttributes += provenanceAttribute
  }

  protected def addProvenanceAttributes(provenanceAttributes: Seq[ProvenanceAttribute]): Unit = {
    this.provenanceAttributes ++= provenanceAttributes
  }

  protected def removeProvenanceAttributes(provenanceAttributes: Seq[ProvenanceAttribute]): Unit = {
    this.provenanceAttributes --= provenanceAttributes
  }

  protected[provenance] def addCompatibilityAttribute(compatibilityAttribute: ProvenanceAttribute): Unit = {
    addProvenanceAttribute(compatibilityAttribute)
    mostRecentCompatibleAttribute = compatibilityAttribute
  }

  protected[provenance] def addCompatibilityAttributes(compatibilityAttributes: Seq[ProvenanceAttribute]): Unit = {
    addProvenanceAttributes(compatibilityAttributes)
    mostRecentCompatibleAttributes = compatibilityAttributes
  }

  protected[provenance] def addSurvivorAttribute(survivorAttribute: ProvenanceAttribute): Unit = {
    addProvenanceAttribute(survivorAttribute)
    mostRecentSurvivorAttribute = survivorAttribute
  }

  protected[provenance] def addSurvivorAttributes(survivorAttributes: Seq[ProvenanceAttribute]): Unit = {
    addProvenanceAttributes(survivorAttributes)
    mostRecentSurvivorAttributes = survivorAttributes
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

  protected[provenance] def getMostRecentCompatibilityAttribute(alternativeIdx: Int):Option[ProvenanceAttribute] = {
    mostRecentCompatibleAttributes.find(attribute => attribute.attributeName.endsWith(Constants.getAlternativeIdxString(alternativeIdx)))
  }

  protected[provenance] def getMostRecentCompatibilityAttributes():Seq[ProvenanceAttribute] = {
    mostRecentCompatibleAttributes
  }

  protected[provenance] def getMostRecentCompatibilityAttributeExpressions(expressions: Seq[NamedExpression]):Seq[NamedExpression] = {
    getExpressionsFromProvenanceAttributes(getMostRecentCompatibilityAttributes(), expressions)
  }


  protected[provenance] def getSurvivedFieldAttribute(oid: Int): Option[ProvenanceAttribute] = {
    provenanceAttributes.find(a => {a.oid == oid && a.attributeName.contains(Constants.SURVIVED_FIELD)})
  }

  protected[provenance] def getMostRecentSurvivedAttribute():Option[ProvenanceAttribute] = {
    if (mostRecentSurvivorAttribute == null) return None
    Some(mostRecentSurvivorAttribute)
  }

  protected[provenance] def getMostRecentSurvivedAttributes():Seq[ProvenanceAttribute] = {
    mostRecentSurvivorAttributes
  }

  protected[provenance] def replaceValidAttributes(validAttributes: Seq[ProvenanceAttribute]): Unit = {
    removeProvenanceAttributes(this.validAttributes)
    addProvenanceAttributes(validAttributes)
    this.validAttributes = validAttributes
  }

  protected[provenance] def replaceOriginalAttributes(originalAttributes: Seq[ProvenanceAttribute]): Unit = {
    removeProvenanceAttributes(this.originalAttributes)
    addProvenanceAttributes(originalAttributes)
    this.originalAttributes = originalAttributes
  }

  protected[provenance] def getValidAttributes(): Seq[ProvenanceAttribute] = {
    validAttributes
  }

  protected[provenance] def getValidAttribute(alternativeID: Int): Option[ProvenanceAttribute] = {
    validAttributes.filter(attr => attr.attributeName.contains(Constants.getAlternativeIdxString(alternativeID))).headOption
  }



  protected[provenance] def getOriginalAttributes(): Seq[ProvenanceAttribute] = {
    originalAttributes
  }

  protected[provenance] def getExpressionFromProvenanceAttribute(attribute: ProvenanceAttribute, expressions: Seq[NamedExpression]): Option[NamedExpression] = {
    expressions.find(ex => ex.name == attribute.attributeName)
  }

  protected[provenance] def getExpressionsFromProvenanceAttributes(attributes: Seq[ProvenanceAttribute], expressions: Seq[NamedExpression]): Seq[NamedExpression] = {
    val attributeExpressions = mutable.ListBuffer.empty[NamedExpression]
    for (attribute <- attributes) {
      attributeExpressions ++= getExpressionFromProvenanceAttribute(attribute, expressions)
    }
    attributeExpressions.toList
  }

  protected[provenance] def getExpressionFromAllProvenanceAttributes(expressions: Seq[NamedExpression]): Seq[NamedExpression] = {
    provenanceAttributes.foldLeft(List.empty[NamedExpression])
    {(provenanceExpressions, attribute) => provenanceExpressions ++ getExpressionFromProvenanceAttribute(attribute, expressions)}
  }

  protected[provenance] def getNestedProvenanceAttributes(): Seq[(ProvenanceAttribute, ProvenanceContext)] = {
    nestedProvenanceContexts.toSeq
  }

  protected[provenance] def getNestedProvenanceAttributes(alternativeId: Int): Seq[(ProvenanceAttribute, ProvenanceContext)] = {
    val alternativeIdString = Constants.getAlternativeIdxString(alternativeId)
    nestedProvenanceContexts.filter{
      case (provenanceAttribute, _) => provenanceAttribute.attributeName.takeRight(4) == alternativeIdString
    }.toSeq

  }




  protected[provenance] def isNestedProvenanceAttribute(provenanceAttribute: ProvenanceAttribute): Boolean = {
    nestedProvenanceContexts.contains(provenanceAttribute)
  }

  protected[provenance] def isNestedProvenanceAttribute(provenanceAttribute: ProvenanceAttribute, alternativeID: Int): Boolean = {
    nestedProvenanceContexts.contains(provenanceAttribute) && provenanceAttribute.attributeName.contains(Constants.getAlternativeIdxString(alternativeID))
  }

  protected[provenance] def isMostRecentCompatibleAttribute(provenanceAttribute: ProvenanceAttribute): Boolean = {
    provenanceAttribute == mostRecentCompatibleAttribute
  }

  protected[provenance] def isMostRecentCompatibleAttribute(provenanceAttribute: ProvenanceAttribute, alternativeID: Int): Boolean = {
    mostRecentCompatibleAttributes.contains(provenanceAttribute) && provenanceAttribute.attributeName.contains(Constants.getAlternativeIdxString(alternativeID))
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

  protected[provenance] def addModifiedOperatorIdtoSchemaAlternative(alternativeId: Int, oid: Int): Unit = {
    val set = operatorsModifiedByAlternatives.getOrElseUpdate(alternativeId, mutable.Set.empty[Int])
    set += oid
  }

}
