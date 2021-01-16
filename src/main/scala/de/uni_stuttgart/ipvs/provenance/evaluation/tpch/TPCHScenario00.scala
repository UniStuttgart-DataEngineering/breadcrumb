package de.uni_stuttgart.ipvs.provenance.evaluation.tpch

import de.uni_stuttgart.ipvs.provenance.evaluation.TestConfiguration
import de.uni_stuttgart.ipvs.provenance.why_not_question.Twig
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{collect_list, struct}

class TPCHScenario00 (spark: SparkSession, testConfiguration: TestConfiguration) extends TPCHScenario(spark, testConfiguration) {
  override def getName(): String = "MergeLineItemIntoOrders"
  import spark.implicits._

  override def whyNotQuestion(): Twig = null

  override def referenceScenario(): DataFrame = {
    val lineitem = loadLineItem()
    val order = loadOrder()
    val customer = loadCustomer()
    /*
    val nestedOrders = loadNestedOrders()
    nestedOrders.show(10, false)
    nestedOrders.printSchema()
    nestedOrders
     */

//      val joined = customer.join(order, $"c_custkey" === $"o_custkey", "left_outer")
//      val nested = joined.filter($"o_orderkey".isNull)

    val joined = order.join(lineitem, $"o_orderkey" === $"l_orderkey")
    val nested = joined.groupBy(order.schema.fieldNames.head, order.schema.fieldNames.tail: _*)
      .agg(collect_list(struct(lineitem.schema.fieldNames.head, lineitem.schema.fieldNames.tail: _*)).as("o_lineitems"))

    //spark.sparkContext.hadoopConfiguration.set("dfs.client.read.shortcircuit.skip.checksum", "true")
    //nested.coalesce(100).write.mode(SaveMode.Overwrite).option("mapreduce.fileoutputcommitter.marksuccessfuljobs","false").json("/user/hadoop/diesterf/data/tpch/nestedOrders/")
      //.option("mapreduce.fileoutputcommitter.marksuccessfuljobs","false")"

    //nested.schema
    /*
    val o_count = order.count()
    val n_count = nested.count()
    println(n_count)
    println(n_count)
    println(n_count == o_count)
     */
    //nested.coalesce(1).write.mode(SaveMode.Overwrite).json(testConfiguration.pathToData + "/nestedorders")


    nested
  }
}