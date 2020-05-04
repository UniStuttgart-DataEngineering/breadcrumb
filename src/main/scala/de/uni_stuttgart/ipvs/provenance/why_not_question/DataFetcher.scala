package de.uni_stuttgart.ipvs.provenance.why_not_question

import org.apache.spark.sql._

import scala.collection.mutable
import org.apache.spark.sql.functions._
import de.uni_stuttgart.ipvs.provenance.nested_why_not.Constants._
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.SchemaSubsetTree

object DataFetcher {

  def apply(dataset: Dataset[_], schemaSubset: SchemaSubsetTree) = new DataFetcher(dataset, schemaSubset)
}

class DataFetcher(dataset: Dataset[_], schemaSubset: SchemaSubsetTree) {

  lazy val dataFetcherUDF = dataset.sparkSession.udf.register("dfu", new DataFetcherUDF().call _)

  def getItems() : Dataset[_] = {
    val udf = dataFetcherUDF
    dataset.filter(udf(struct(dataset.columns.toSeq.map(col(_)): _*), typedLit(schemaSubset.serialize())))
  }

  def markItems(columnName: String) : Dataset[_] = {
    val udf = dataFetcherUDF
    dataset.withColumn(columnName, udf(struct(dataset.columns.toSeq.map(col(_)): _*), typedLit(schemaSubset.serialize())))
  }


}
