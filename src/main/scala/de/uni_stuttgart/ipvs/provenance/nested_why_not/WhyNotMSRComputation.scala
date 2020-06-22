package de.uni_stuttgart.ipvs.provenance.nested_why_not

import de.uni_stuttgart.ipvs.provenance.why_not_question.{DataFetcherUDF, Twig}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, struct, count}

object WhyNotMSRComputation {

  private val operatorListName = "pickyOperators"
  private val compatibleCountName = "compatibleCount"

  def computeMSR(dataFrame: DataFrame, provenanceContext: ProvenanceContext): DataFrame = {

    val lastCompatibleAttribute = provenanceContext.getMostRecentCompatibilityAttribute()
    val compatibleName = lastCompatibleAttribute.get.attributeName

    val compatiblesOnly = dataFrame.filter(dataFrame.col(compatibleName) === true)


    val msrUDF = dataFrame.sparkSession.udf.register("msr", new MSRComputationUDF().call _)
    val survivorColumns = compatiblesOnly.columns.filter(name => name.contains(Constants.SURVIVED_FIELD))
    val survivorsOnly = compatiblesOnly.select(survivorColumns.map(col): _*)
    val survivorsWithLostColumns = survivorsOnly.withColumn(operatorListName, msrUDF(struct(survivorsOnly.columns.toSeq.map(col(_)): _*)))
    val result = survivorsWithLostColumns.groupBy(operatorListName).agg(count(survivorsWithLostColumns.columns.head).alias(compatibleCountName))
    result

  }

}
