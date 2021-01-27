package de.uni_stuttgart.ipvs.provenance.evaluation.tpch

import de.uni_stuttgart.ipvs.provenance.evaluation.TestConfiguration
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.{PrimarySchemaSubsetTree, SchemaNode, SchemaSubsetTree}
import de.uni_stuttgart.ipvs.provenance.why_not_question.Twig
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.functions.{countDistinct, explode, count}
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

Result over sample for mq:
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
|[0003, 0007]  |1              |000042     |
|[0007]        |1              |000042     |
|[0003, 0007]  |1              |000046     |
|[0007]        |1              |000046     |
|[0003, 0007]  |1              |000044     |
|[0007]        |1              |000044     |
|[0003, 0007]  |1              |000049     |
|[0007]        |1              |000049     |
|[0003, 0007]  |1              |000048     |
|[0007]        |1              |000048     |
+--------------+---------------+-----------+

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
    val nestedOrders2 = loadNestedOrders()
    val filterOrderDate2 = nestedOrders2.filter($"o_orderdate" >= "1993-07-01" && $"o_orderdate" < "1993-10-01")
    val nestedOrders = loadNestedOrders()

    val filterOrderDate = nestedOrders.filter($"o_orderdate" >= "1993-07-01" && $"o_orderdate" < "1993-10-01") // oid = 3
    val flattenOrd = filterOrderDate.withColumn("lineitem", explode($"o_lineitems"))
    //    val projectOrdKey = filterLine.select($"l_orderkey").distinct() // Not supported
    val filterCommitShipDate = flattenOrd.filter($"lineitem.l_shipdate" < $"lineitem.l_receiptdate") // SA: l_shipdate -> l_commitdate // oid = 7
    val projectExpr = filterCommitShipDate.select($"lineitem.l_orderkey".alias("l_orderkey"), $"lineitem.l_comment".alias("l_comment"))
    val projectOrdKey = projectExpr.groupBy($"l_orderkey").agg(count($"l_comment").alias("temp"))
    val joinOrdLine = filterOrderDate2.join(projectOrdKey, $"o_orderkey" === $"l_orderkey")
    val res = joinOrdLine.groupBy($"o_orderpriority").agg(count($"o_orderkey").alias("order_count"))

    res
  }

  def nestedScenarioWithShipToCommitDateWithSmall: DataFrame = {
    val nestedOrders = loadNestedOrders001()

    val filterOrderDate = nestedOrders.filter($"o_orderdate" >= "1993-07-01" && $"o_orderdate" < "1993-10-01")
    val flattenOrd = filterOrderDate.withColumn("lineitem", explode($"o_lineitems"))
    //    val projectOrdKey = filterLine.select($"l_orderkey").distinct() // Not supported
    val filterCommitShipDate = flattenOrd.filter($"lineitem.l_shipdate" < $"lineitem.l_receiptdate") // SA: l_shipdate -> l_commitdate
    val projectExpr = filterCommitShipDate.select($"lineitem.l_orderkey".alias("l_orderkey"), $"lineitem.l_comment".alias("l_comment"))
    val projectOrdKey = projectExpr.groupBy($"l_orderkey").agg(count($"l_comment").alias("temp"))
    val joinOrdLine = filterOrderDate.join(projectOrdKey, $"o_orderkey" === $"l_orderkey")
    val res = joinOrdLine.groupBy($"o_orderpriority").agg(count($"o_orderkey").alias("order_count"))

    res
  }


  override def referenceScenario: DataFrame = {
//    return unmodifiedNestedReferenceScenario
    return nestedScenarioWithShipToCommitDate
//    return nestedScenarioWithShipToCommitDateWithSmall
  }

  override def getName(): String = "TPCH104"

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

  var modified = true

  override def computeAlternatives(backtracedWhyNotQuestion: SchemaSubsetTree, input: LeafNode): PrimarySchemaSubsetTree = {
    val primaryTree = super.computeAlternatives(backtracedWhyNotQuestion, input)
    //val nestedOrder = input.asInstanceOf[LogicalRelation].relation.asInstanceOf[HadoopFsRelation].location.rootPaths.head.toUri.toString.contains("nestedOrders")

    if(!modified) {
      //TODO: need to check
      NestedOrdersAlternatives.createAlternativesWith2Permutations(primaryTree,
        Seq("o_shippriority", "o_orderpriority"), Seq("l_shipdate", "l_receiptdate", "l_commitdate"))
      modified = true

//      val saSize = testConfiguration.schemaAlternativeSize
//      createAlternatives(primaryTree, saSize)
//
//      for (i <- 0 until saSize) {
//        replaceDate(primaryTree.alternatives(i).rootNode)
//      }
    } else {
      modified = false
    }

    primaryTree
  }

//  def replaceDate(node: SchemaNode): Unit ={
//    if (node.name == "l_shipdate" && node.children.isEmpty) {
//      node.name = "l_commitdate"
//      node.modified = true
//      return
//    }
//    for (child <- node.children){
//      replaceDate(child)
//    }
//  }
}
