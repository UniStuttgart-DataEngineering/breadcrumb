package de.uni_stuttgart.ipvs.provenance.why_not_question

import org.apache.spark.sql._

import scala.collection.mutable
import org.apache.spark.sql.functions._
import de.uni_stuttgart.ipvs.provenance.nested_why_not.Constants._

object DataFetcher {

  def apply(dataset: Dataset[_], schemaMatch: SchemaMatch) = new DataFetcher(dataset, schemaMatch)
}

class DataFetcher(dataset: Dataset[_], schemaMatch: SchemaMatch) {

  lazy val dataFetcherUDF = dataset.sparkSession.udf.register("dfu", new DataFetcherUDF().call _)

  def getItems() : Dataset[_] = {
    val udf = dataFetcherUDF
    dataset.filter(udf(struct(dataset.columns.toSeq.map(col(_)): _*), typedLit(schemaMatch.serialize())))
  }

  def markItems(columnName: String) : Dataset[_] = {
    val udf = dataFetcherUDF
    dataset.withColumn(columnName, udf(struct(dataset.columns.toSeq.map(col(_)): _*), typedLit(schemaMatch.serialize())))
  }


}
