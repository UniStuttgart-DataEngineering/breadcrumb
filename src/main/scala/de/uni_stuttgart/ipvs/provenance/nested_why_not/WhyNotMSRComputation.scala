package de.uni_stuttgart.ipvs.provenance.nested_why_not

import de.uni_stuttgart.ipvs.provenance.why_not_question.{DataFetcherUDF, Twig}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, count, countDistinct, explode, monotonically_increasing_id, struct}
import org.apache.spark.sql.types.LongType

object WhyNotMSRComputation {

  private val operatorListName = "pickyOperators"
  private val compatibleCountName = "compatibleCount"
  private val intermediateTupleUnnestingName = "flattened"

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


  def selectNestedProvenanceAttributes(dataFrame: DataFrame,
                                       provenanceAttribute: ProvenanceAttribute,
                                       provenanceContext: ProvenanceContext): DataFrame = {
    var resultDataFrame = dataFrame
    resultDataFrame = resultDataFrame
      .withColumn(intermediateTupleUnnestingName,
        explode(resultDataFrame.col(provenanceAttribute.attributeName)))

    val relevantColumns = getRelevantColumns(provenanceContext, resultDataFrame)
    resultDataFrame = resultDataFrame.select(relevantColumns.map(col): _*)
    val filterAttribute = provenanceContext.getMostRecentCompatibilityAttribute().get.attributeName
    resultDataFrame = resultDataFrame.filter(resultDataFrame.col(filterAttribute) === true)
    resultDataFrame = resultDataFrame.drop(filterAttribute)
    flattenNestedProvenanceCollections(resultDataFrame, provenanceContext)
  }

  protected def getRelevantColumns(provenanceContext: ProvenanceContext, resultDataFrame: DataFrame): Seq[String] = {
    val existingFlattenedColumnsToBePreserved = resultDataFrame
      .columns
      .filter(name => name != intermediateTupleUnnestingName)
    val newRelevantColumns = provenanceContext.provenanceAttributes
      .withFilter(attribute =>
        provenanceContext.isSurvivedAttribute(attribute)
          || provenanceContext.isMostRecentCompatibleAttribute(attribute)
          || provenanceContext.isNestedProvenanceAttribute(attribute))
      .map(attribute => s"$intermediateTupleUnnestingName.${attribute.attributeName}")
    val relevantColumns: Seq[String] = existingFlattenedColumnsToBePreserved ++ newRelevantColumns
    relevantColumns
  }

}
