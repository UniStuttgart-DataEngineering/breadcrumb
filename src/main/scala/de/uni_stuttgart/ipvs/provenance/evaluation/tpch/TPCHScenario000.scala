package de.uni_stuttgart.ipvs.provenance.evaluation.tpch

import de.uni_stuttgart.ipvs.provenance.evaluation.TestConfiguration
import de.uni_stuttgart.ipvs.provenance.why_not_question.Twig
import org.apache.spark.sql.functions.{collect_list, struct, when}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

class TPCHScenario000(spark: SparkSession, testConfiguration: TestConfiguration) extends TPCHScenario(spark, testConfiguration) {
  override def getName(): String = "MergeNestedOrdersIntoCustomer"
  import spark.implicits._

  override def whyNotQuestion(): Twig = null

  override def referenceScenario(): DataFrame = {
    val customer = loadCustomer()
    val nestedOrders = loadNestedOrders()

    val joined = customer.join(nestedOrders, $"c_custkey" === $"o_custkey", "left_outer")
    val temp = joined.withColumn("nestedOrder",
      when($"o_orderkey".isNull, null)
        .otherwise(struct(nestedOrders.schema.fieldNames.head, nestedOrders.schema.fieldNames.tail: _*)))
    var nested = temp.groupBy(customer.schema.fieldNames.head, customer.schema.fieldNames.tail: _*)
      .agg(collect_list($"nestedOrder").as("c_orders"))
    //spark.sparkContext.hadoopConfiguration.set("dfs.client.read.shortcircuit.skip.checksum", "true")
//    nested.coalesce(1).write.mode(SaveMode.Overwrite).option("mapreduce.fileoutputcommitter.marksuccessfuljobs","false")
//      .json(testConfiguration.pathToData + "/nestedcustomer")
//    nested.coalesce(10).write.mode(SaveMode.Overwrite).option("mapreduce.fileoutputcommitter.marksuccessfuljobs","false")
//      .json("/user/hadoop/diesterf/data/tpch/nestedCustomers10/")
//      //.option("mapreduce.fileoutputcommitter.marksuccessfuljobs","false")"

    //nested.schema
    /*
    val o_count = order.count()
    val n_count = nested.count()
    println(n_count)
    println(n_count)
    println(n_count == o_count)
     */

//    nested = nested.filter($"c_custkey".isNotNull)
//    println(nested.count())
    nested.coalesce(1).write.mode(SaveMode.Overwrite).json(testConfiguration.pathToData + "/nestedcustomer")
    nested
  }
}
