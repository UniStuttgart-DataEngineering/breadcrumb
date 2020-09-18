package de.uni_stuttgart.ipvs.provenance.nested_why_not

import de.uni_stuttgart.ipvs.provenance.schema_alternatives.SchemaSubsetTree
import de.uni_stuttgart.ipvs.provenance.why_not_question.{DataFetcherUDF, Twig}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, count, countDistinct, explode, greatest, monotonically_increasing_id, struct, typedLit}
import org.apache.spark.sql.types.LongType

import scala.collection.mutable

object WhyNotMSRComputation {

  private val operatorListName = "pickyOperators"
  private val compatibleCountName = "compatibleCount"
  private val intermediateTupleUnnestingName = "flattened"

  private var relevantAlternativeIds = mutable.Set(Int)

  def computeMSR(dataFrame: DataFrame, provenanceContext: ProvenanceContext): DataFrame = {
    val msrUDF = dataFrame.sparkSession.udf.register("msr", new MSRComputationUDF().call _)
//    dataFrame.show(false)

    val provenanceAttributesOnly = dataFrame.select(
      provenanceContext.getProvenanceAttributes().map(attribute => col(attribute.attributeName)): _*)
    val (provenanceWithUid, uidAttribute) = addUID(provenanceAttributesOnly, provenanceContext)
//    provenanceWithUid.show(false)


    val lastCompatibleAttribute = provenanceContext.getMostRecentCompatibilityAttribute()
    val compatibleName = lastCompatibleAttribute.get.attributeName

    val compatiblesOnly = provenanceWithUid.filter(dataFrame.col(compatibleName) === true)
    val flattenedCompatiblesOnly = flattenNestedProvenanceCollections(compatiblesOnly, provenanceContext)

    val survivorColumns = flattenedCompatiblesOnly.columns.filter(name => Constants.isSurvivedField(name) || Constants.isIDField(name))
    val survivorsOnly = flattenedCompatiblesOnly.select(survivorColumns.map(col): _*)
    val survivorsWithLostColumns = survivorsOnly.withColumn(operatorListName, msrUDF(struct(survivorsOnly.columns.toSeq.map(col(_)): _*)))
//    survivorsWithLostColumns.show(false)
    val result = survivorsWithLostColumns.groupBy(operatorListName).agg(countDistinct(col(uidAttribute.attributeName)).alias(compatibleCountName))
    result

  }

  def getModifiedOps(provenanceContext: ProvenanceContext, relevantIds: Set[Int]) : Seq[Int] = {
    provenanceContext.operatorsModifiedByAlternatives.filter(item => relevantIds.contains(item._1))
      .foldLeft(mutable.Set.empty[Int])((res, item) => res ++= item._2).toSeq
  }

  def computeMSRForAlternative(dataFrame: DataFrame, provenanceContext: ProvenanceContext, alternative: SchemaSubsetTree, uidAttribute: ProvenanceAttribute): DataFrame = {
    val msrUDF = dataFrame.sparkSession.udf.register("msr", new MSRComputationUDF().call _)
    val msrUDFWithAlternatives = dataFrame.sparkSession.udf.register("msr", new MSRComputationAlternativeUDF().call _)
    val relevantIds : Set[Int] = getAllRelevantIds(provenanceContext, alternative.id, mutable.Set.empty[Int]).toSet
    val modifiedOps = getModifiedOps(provenanceContext, relevantIds)
    val validAttribute = provenanceContext.getValidAttributes().filter(attribute => attribute.attributeName.contains(Constants.getAlternativeIdxString(alternative.id))).head
    val compatibleAttribute = provenanceContext.getMostRecentCompatibilityAttributes().filter(attribute => attribute.attributeName.contains(Constants.getAlternativeIdxString(alternative.id))).head

    val validColumn = dataFrame.columns.filter(name => name == validAttribute.attributeName).head
    val compatibleColumn = dataFrame.columns.filter(name => name == compatibleAttribute.attributeName).head
    var relevantUnflattenedTuples = dataFrame.filter(col(validColumn) === true && col(compatibleColumn) === true)
    //relevantUnflattenedTuples.show(10, false)
    //relevantUnflattenedTuples.printSchema()
    //relevantUnflattenedTuples.show()
    relevantUnflattenedTuples = relevantUnflattenedTuples.drop(validColumn).drop(compatibleColumn)
    val flattenedCompatiblesOnly = flattenNestedProvenanceCollections(relevantUnflattenedTuples, provenanceContext, alternative.id)


    val filteredColumns = flattenedCompatiblesOnly.columns.filter(col => Constants.isSurvivedField(col, relevantIds) || Constants.isIDField(col))
    val survivedCompatiblesOnly = flattenedCompatiblesOnly.select(filteredColumns.map(col(_)): _*)
    var pickyOperators = survivedCompatiblesOnly.withColumn(operatorListName, msrUDFWithAlternatives(struct(survivedCompatiblesOnly.columns.toSeq.map(col(_)): _*), typedLit(modifiedOps)))
    pickyOperators = pickyOperators.groupBy(operatorListName).agg(countDistinct(col(uidAttribute.attributeName)).alias(compatibleCountName))
    pickyOperators
  }

  def filterForAllCompatibles(dataFrame: DataFrame, columnNames: Seq[String]): DataFrame = {
    dataFrame.filter(greatest(columnNames.map(col): _*) === true)
  }

  def computeMSRForSchemaAlternatives(dataFrame: DataFrame, provenanceContext: ProvenanceContext): Map[Int, DataFrame] = {
    //dataFrame.printSchema()
    //dataFrame.show(false)
    import dataFrame.sparkSession.implicits._
    //val constraint = "Scalable algorithms for scholarly figure mining and semantics"
    //val column = provenanceContext.getMostRecentCompatibilityAttribute(provenanceContext.primarySchemaAlternative.id).get.attributeName
    //dataFrame.filter(col(column) === true)/*.select(col("author"), col(column))*/.show(20, false)
    val provenanceAttributesOnly = dataFrame.select(
      provenanceContext.getProvenanceAttributes().map(attribute => col(attribute.attributeName)): _*)
    val lastCompatibleAttribute = provenanceContext.getMostRecentCompatibilityAttributes()
    val (provenanceWithUid, uidAttribute) = addUID(provenanceAttributesOnly, provenanceContext)
    provenanceWithUid.cache()
    //provenanceWithUid.printSchema()
    //provenanceWithUid.show(false)
    val compatibleNames = lastCompatibleAttribute.map{c => c.attributeName}
    val compatiblesOnly = filterForAllCompatibles(provenanceWithUid, compatibleNames) //tuple based only
    val pickyOperators = mutable.Map.empty[Int, DataFrame]
    for (alternative <- provenanceContext.primarySchemaAlternative.getAllAlternatives()){
      pickyOperators.put(alternative.id, computeMSRForAlternative(compatiblesOnly, provenanceContext, alternative, uidAttribute))
    }
    pickyOperators.toMap
  }

  def getAllRelevantIds(provenanceContext: ProvenanceContext, currentId: Int, ids: mutable.Set[Int]): mutable.Set[Int] = {
    if (ids.contains(currentId)){
      return ids
    }
    ids += currentId
    val (left, right) = provenanceContext.associatedIds.getOrElse(currentId, (currentId, currentId))
    ids ++= getAllRelevantIds(provenanceContext, left, ids)
    ids ++= getAllRelevantIds(provenanceContext, right, ids)
    ids
  }

  def addUID(dataFrame: DataFrame, provenanceContext: ProvenanceContext): (DataFrame, ProvenanceAttribute) = {
    val provenanceAttribute = ProvenanceAttribute(-1, Constants.PROVENANCE_ID, LongType)
    provenanceContext.addIDAttribute(provenanceAttribute)
    val res = dataFrame.withColumn(provenanceAttribute.attributeName, monotonically_increasing_id)
    (res, provenanceAttribute)
  }



  def flattenNestedProvenanceCollections(dataFrame: DataFrame, provenanceContext: ProvenanceContext): DataFrame = {
    var resultDataFrame = dataFrame
    for ((nestedAttribute, nestedProvenanceCollection) <- provenanceContext.getNestedProvenanceAttributes()) {
      resultDataFrame = selectNestedProvenanceAttributes(resultDataFrame, nestedAttribute, nestedProvenanceCollection)
    }
    //resultDataFrame.show()
    resultDataFrame
  }

  def flattenNestedProvenanceCollections(dataFrame: DataFrame, provenanceContext: ProvenanceContext, alternativeId: Int): DataFrame = {
    var resultDataFrame = dataFrame
    for ((nestedAttribute, nestedProvenanceCollection) <- provenanceContext.getNestedProvenanceAttributes(alternativeId)) {
      resultDataFrame = selectNestedProvenanceAttributes(resultDataFrame, nestedAttribute, nestedProvenanceCollection, alternativeId)
    }
    //resultDataFrame.show()
    resultDataFrame
  }


  def selectNestedProvenanceAttributes(dataFrame: DataFrame,
                                       provenanceAttribute: ProvenanceAttribute,
                                       provenanceContext: ProvenanceContext, alternativeId: Int = -1): DataFrame = {
    var resultDataFrame = dataFrame
    resultDataFrame = resultDataFrame
      .withColumn(intermediateTupleUnnestingName,
        explode(resultDataFrame.col(provenanceAttribute.attributeName)))

    val relevantColumns = getRelevantColumns(provenanceContext, resultDataFrame, alternativeId)
    resultDataFrame = resultDataFrame.select(relevantColumns.map(col): _*)
    val filterAttribute = if (alternativeId < 0) {
      provenanceContext.getMostRecentCompatibilityAttribute().get.attributeName
    } else {
      provenanceContext.getMostRecentCompatibilityAttribute(alternativeId).get.attributeName
    }
    var filterCondition = resultDataFrame.col(filterAttribute) === true
    if (alternativeId > 0) {
      val validAttribute = provenanceContext.getValidAttribute(alternativeId).get.attributeName
      filterCondition = filterCondition && resultDataFrame.col(validAttribute) === true
    }
    resultDataFrame = resultDataFrame.filter(filterCondition)
    resultDataFrame = resultDataFrame.drop(filterAttribute)
    if (alternativeId < 0) {
      flattenNestedProvenanceCollections(resultDataFrame, provenanceContext)
    } else {
      flattenNestedProvenanceCollections(resultDataFrame, provenanceContext, alternativeId)
    }

  }

  protected def getRelevantColumns(provenanceContext: ProvenanceContext, resultDataFrame: DataFrame, alternativeId: Int = -1): Seq[String] = {
    val existingFlattenedColumnsToBePreserved = resultDataFrame
      .columns
      .filter(name => name != intermediateTupleUnnestingName)
    val newRelevantColumns = provenanceContext.provenanceAttributes
      .withFilter(attribute =>
        Constants.isSurvivedField(attribute.attributeName)
          || provenanceContext.isMostRecentCompatibleAttribute(attribute)
          || provenanceContext.isMostRecentCompatibleAttribute(attribute, alternativeId)
          || provenanceContext.isNestedProvenanceAttribute(attribute)
          || provenanceContext.isNestedProvenanceAttribute(attribute, alternativeId)
          || Constants.isValidField(attribute.attributeName, alternativeId)
          )
      .map(attribute => s"$intermediateTupleUnnestingName.${attribute.attributeName}")
    val relevantColumns: Seq[String] = existingFlattenedColumnsToBePreserved ++ newRelevantColumns
    relevantColumns
  }

}
