package de.uni_stuttgart.ipvs.provenance.evaluation.tpch

import de.uni_stuttgart.ipvs.provenance.evaluation.TestConfiguration
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.{PrimarySchemaSubsetTree, SchemaNode, SchemaSubsetTree}
import de.uni_stuttgart.ipvs.provenance.why_not_question.Twig
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.functions.{countDistinct, explode}
import org.apache.spark.sql.{DataFrame, SparkSession}

class TPCHScenario104(spark: SparkSession, testConfiguration: TestConfiguration) extends TPCHScenario(spark, testConfiguration) {


  import spark.implicits._

//  val decrease = udf { (x: Double, y: Double) => x * (1 - y) }
//  val increase = udf { (x: Double, y: Double) => x * (1 + y) }

//This query computes order count per order priority for the 3rd quarter (0701 - 0930) in 2013
//Alternatives: l_shipdate -> l_receiptdate
//WN-Question: Why is a value not bigger than 10000
//Assumed possible answers: filter

/*
Original result:
+---------------+-----------+
|o_orderpriority|order_count|
+---------------+-----------+
|5-LOW          |10487      |
|3-MEDIUM       |10410      |
|1-URGENT       |10594      |
|4-NOT SPECIFIED|10556      |
|2-HIGH         |10476      |
+---------------+-----------+

over sample
+---------------+-----------+
|o_orderpriority|order_count|
+---------------+-----------+
|5-LOW          |18         |
|3-MEDIUM       |5          |
|1-URGENT       |14         |
|4-NOT SPECIFIED|4          |
|2-HIGH         |10         |
+---------------+-----------+

TODO:
 1) Distinct & countDistinct not supported yet
*/

  def unmodifiedNestedReferenceScenario: DataFrame = {
    val nestedOrders = loadNestedOrders()

    val filterOrderDate = nestedOrders.filter($"o_orderdate" >= "1993-07-01" && $"o_orderdate" < "1993-10-01")
    val flattenOrd = filterOrderDate.withColumn("lineitem", explode($"o_lineitems"))
//    val projectOrdKey = filterLine.select($"l_orderkey").distinct()
//    val projectOrdKey = filterLine.groupBy($"l_orderkey").agg(count($"l_comment"))
    val filterCommitShipDate = flattenOrd.filter($"lineitem.l_commitdate" < $"lineitem.l_receiptdate")
    val res = filterCommitShipDate.groupBy($"o_orderpriority").agg(countDistinct($"o_orderkey").alias("order_count"))

    res
  }

  def nestedScenarioWithShipToCommitDate: DataFrame = {
    val nestedOrders = loadNestedOrders()

    val filterOrderDate = nestedOrders.filter($"o_orderdate" >= "1993-07-01" && $"o_orderdate" < "1993-10-01")
    val flattenOrd = filterOrderDate.withColumn("lineitem", explode($"o_lineitems"))
    //    val projectOrdKey = filterLine.select($"l_orderkey").distinct() // Not supported
    //    val projectOrdKey = filterLine.groupBy($"l_orderkey").agg(count($"l_comment"))  // Not supported
    val filterCommitShipDate = flattenOrd.filter($"lineitem.l_shipdate" < $"lineitem.l_receiptdate") // SA: l_shipdate -> l_commitdate
    val res = filterCommitShipDate.groupBy($"o_orderpriority").agg(countDistinct($"o_orderkey").alias("order_count"))

    res
  }


  override def referenceScenario: DataFrame = {
//    return unmodifiedNestedReferenceScenario
        return nestedScenarioWithShipToCommitDate
  }

  override def getName(): String = "TPCH104"

  override def whyNotQuestion: Twig = {
    var twig = new Twig()
    val root = twig.createNode("root")
    val priority = twig.createNode("o_orderpriority", 1, 1, "1-URGENT")
    val count = twig.createNode("order_count", 1, 1, "ltltltlt11000")
    // Only for sample data
//    val priority = twig.createNode("o_orderpriority", 1, 1, "3-MEDIUM")
//    val count = twig.createNode("order_count", 1, 1, "ltltltlt6")
    twig = twig.createEdge(root, priority, false)
    twig = twig.createEdge(root, count, false)
    twig.validate.get
  }


  override def computeAlternatives(backtracedWhyNotQuestion: SchemaSubsetTree, input: LeafNode): PrimarySchemaSubsetTree = {
    val primaryTree = super.computeAlternatives(backtracedWhyNotQuestion, input)
    val saSize = testConfiguration.schemaAlternativeSize
    createAlternatives(primaryTree, saSize)

    for (i <- 0 until saSize by 2) {
      replaceDate(primaryTree.alternatives(i).rootNode)
    }

    primaryTree
  }

  def replaceDate(node: SchemaNode): Unit ={
    if (node.name == "l_shipdate" && node.children.isEmpty) {
      node.name = "l_commitdate"
      node.modified = true
      return
    }
    for (child <- node.children){
      replaceDate(child)
    }
  }
}
