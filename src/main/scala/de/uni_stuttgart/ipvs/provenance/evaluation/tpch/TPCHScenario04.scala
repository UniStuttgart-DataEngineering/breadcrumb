package de.uni_stuttgart.ipvs.provenance.evaluation.tpch

import de.uni_stuttgart.ipvs.provenance.evaluation.TestConfiguration
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.{PrimarySchemaSubsetTree, SchemaNode, SchemaSubsetTree}
import de.uni_stuttgart.ipvs.provenance.why_not_question.Twig
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.functions.{avg, count, explode, max, min, sum, udf, countDistinct}
import org.apache.spark.sql.{DataFrame, SparkSession}

class TPCHScenario04(spark: SparkSession, testConfiguration: TestConfiguration) extends TPCHScenario(spark, testConfiguration) {


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

Result for mq:
+---------------+-----------+
|o_orderpriority|order_count|
+---------------+-----------+
|5-LOW          |11398      |
|3-MEDIUM       |11343      |
|1-URGENT       |11522      |
|4-NOT SPECIFIED|11495      |
|2-HIGH         |11460      |
+---------------+-----------+

Result over sample:
+---------------+-----------+
|o_orderpriority|order_count|
+---------------+-----------+
|5-LOW          |18         |
|3-MEDIUM       |5          |
|1-URGENT       |14         |
|4-NOT SPECIFIED|4          |
|2-HIGH         |10         |
+---------------+-----------+

Result over sample for mq:
+---------------+-----------+
|o_orderpriority|order_count|
+---------------+-----------+
|5-LOW          |68         |
|3-MEDIUM       |16         |
|1-URGENT       |61         |
|4-NOT SPECIFIED|21         |
|2-HIGH         |45         |
+---------------+-----------+

+---------------+-----------+
|o_orderpriority|order_count|
+---------------+-----------+
|5-LOW          |18         |
|3-MEDIUM       |7          |
|1-URGENT       |14         |
|4-NOT SPECIFIED|4          |
|2-HIGH         |11         |
+---------------+-----------+

Explanation over sample for mq:
+--------------+---------------+-----------+
|pickyOperators|compatibleCount|alternative|
+--------------+---------------+-----------+
|[0003, 0006]  |1              |000022     |
|[0006]        |1              |000022     |
+--------------+---------------+-----------+

TODO:
 1) Distinct & countDistinct not supported yet
 2) No explanation is retured with "left semi join"
  - The order count is computed with ignoring order data condition at least
*/


  def unmodifiedReferenceScenario: DataFrame = {
    val orders = loadOrder()
    val lineitem = loadLineItem()

    val filterOrdDate = orders.filter($"o_orderdate".between("1993-07-01", "1993-09-30"))
    val filterLine = lineitem.filter($"l_commitdate" < $"l_receiptdate")
//    val projectOrdKey = filterLine.select($"l_orderkey").distinct()
//    val projectOrdKey = filterLine.groupBy($"l_orderkey").agg(count($"l_comment"))
    val joinOrdLine = filterOrdDate.join(filterLine, $"o_orderkey" === $"l_orderkey", "left_semi")
    val res = joinOrdLine.groupBy($"o_orderpriority").agg(count($"o_orderkey").alias("order_count"))

    res
  }

  def flatScenarioWithShipToCommitDate: DataFrame = {
    val orders = loadOrder()
    val lineitem = loadLineItem()

    val filterOrdDate = orders.filter($"o_orderdate".between("1993-07-01", "1993-09-30")) // oid=3
    val filterLine = lineitem.filter($"l_shipdate" < $"l_receiptdate") // SA l_shipdate -> l_commitdate // oid=6
//    val projectOrdKey = filterLine.select($"l_orderkey").distinct() // Not supported
    val projectOrdKey = filterLine.groupBy($"l_orderkey").agg(count($"l_comment").alias("temp"))
    val joinOrdLine = filterOrdDate.join(projectOrdKey, $"o_orderkey" === $"l_orderkey") // left_semi -> natural join
    val res = joinOrdLine.groupBy($"o_orderpriority").agg(count($"o_orderkey").alias("order_count"))

    res
  }

  override def referenceScenario: DataFrame = {
//        return unmodifiedReferenceScenario
    return flatScenarioWithShipToCommitDate
  }

  override def getName(): String = "TPCH04"

  override def whyNotQuestion: Twig = {
    var twig = new Twig()
    val root = twig.createNode("root")
    val priority = twig.createNode("o_orderpriority", 1, 1, "3-MEDIUM")
    val count = twig.createNode("order_count", 1, 1, "ltltltlt11000")
//    val count = twig.createNode("order_count", 1, 1, "ltltltlt6") // for sample data
    twig = twig.createEdge(root, priority, false)
    twig = twig.createEdge(root, count, false)
    twig.validate.get
  }


  override def computeAlternatives(backtracedWhyNotQuestion: SchemaSubsetTree, input: LeafNode): PrimarySchemaSubsetTree = {
    val primaryTree = super.computeAlternatives(backtracedWhyNotQuestion, input)
    val lineitem = input.asInstanceOf[LogicalRelation].relation.asInstanceOf[HadoopFsRelation].location.rootPaths.head.toUri.toString.contains("lineitem")
    val order = input.asInstanceOf[LogicalRelation].relation.asInstanceOf[HadoopFsRelation].location.rootPaths.head.toUri.toString.contains("orders")

    if(order) {
      OrdersAlternatives.createAllAlternatives(primaryTree)
    }

    if(lineitem) {
//      LineItemAlternatives().createAlternativesWith2Permutations(primaryTree, Seq("l_shipdate", "l_receiptdate", "l_commitdate"))

      val saSize = testConfiguration.schemaAlternativeSize
      createAlternatives(primaryTree, saSize)

      for (i <- 0 until saSize) {
        replaceDate(primaryTree.alternatives(i).rootNode)
      }
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
