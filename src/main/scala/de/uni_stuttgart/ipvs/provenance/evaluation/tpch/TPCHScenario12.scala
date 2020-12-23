package de.uni_stuttgart.ipvs.provenance.evaluation.tpch

import de.uni_stuttgart.ipvs.provenance.evaluation.TestConfiguration
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.{PrimarySchemaSubsetTree, SchemaNode, SchemaSubsetTree}
import de.uni_stuttgart.ipvs.provenance.why_not_question.Twig
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.functions.{count, explode, sum, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

class TPCHScenario12(spark: SparkSession, testConfiguration: TestConfiguration) extends TPCHScenario(spark, testConfiguration) {


  import spark.implicits._

//  val decrease = udf { (x: Double, y: Double) => x * (1 - y) }
//  val increase = udf { (x: Double, y: Double) => x * (1 + y) }

  /*
Original result:
+----------+---------------+--------------+
|l_shipmode|high_line_count|low_line_count|
+----------+---------------+--------------+
|MAIL      |6202           |9324          |
|SHIP      |6200           |9262          |
+----------+---------------+--------------+

Rewrite without SA:
CASE WHEN ((o_orderpriority#132 = 1-URGENT) || (o_orderpriority#132 = 2-HIGH)) THEN 1 ELSE 0 END (of class org.apache.spark.sql.catalyst.expressions.CaseWhen)
scala.MatchError: CASE WHEN ((o_orderpriority#132 = 1-URGENT) || (o_orderpriority#132 = 2-HIGH)) THEN 1 ELSE 0 END (of class org.apache.spark.sql.catalyst.expressions.CaseWhen)

Rewrite with SA + MSR:
CASE WHEN ((o_orderpriority#132 = 1-URGENT) || (o_orderpriority#132 = 2-HIGH)) THEN 1 ELSE 0 END (of class org.apache.spark.sql.catalyst.expressions.CaseWhen)
scala.MatchError: CASE WHEN ((o_orderpriority#132 = 1-URGENT) || (o_orderpriority#132 = 2-HIGH)) THEN 1 ELSE 0 END (of class org.apache.spark.sql.catalyst.expressions.CaseWhen)
*/

  override def referenceScenario: DataFrame = {
    val orders = loadOrder()
    val lineitem = loadLineItem()
    val nestedOrders = loadNestedOrders()

//    // Original query
//    val filterShipmode = lineitem.filter($"l_shipmode" ===  "MAIL" || $"l_shipmode" ===  "SHIP")
//    val filterCommitReceiptDate = filterShipmode.filter($"l_commitdate" < $"l_receiptdate")
//    val filterShipCommitDate = filterCommitReceiptDate.filter($"l_shipdate" < $"l_commitdate")
//    val filterReceiptDate = filterShipCommitDate.filter($"l_receiptdate".between("1994-01-01", "1994-12-31"))
//    val joinOrdLine = orders.join(filterReceiptDate, $"o_orderkey" === $"l_orderkey")
//    var res = joinOrdLine.groupBy($"l_shipmode")
//        .agg(sum(when($"o_orderpriority" === "1-URGENT" || $"o_orderpriority" === "2-HIGH", 1).otherwise(0)).alias("high_line_count"),
//        sum(when($"o_orderpriority" =!= "1-URGENT" && $"o_orderpriority" =!= "2-HIGH", 1).otherwise(0)).alias("low_line_count"))

    // New query
    val flattenLine = nestedOrders.withColumn("lineitem", explode($"o_lineitems"))
    val filterShipmode = flattenLine.filter($"lineitem.l_shipmode" ===  "MAIL" || $"lineitem.l_shipmode" ===  "SHIP")
    val filterCommReceiptDate = filterShipmode.filter($"lineitem.l_commitdate" < $"lineitem.l_receiptdate") //TODO: can we have "l_receiptdate <-> l_commitdate"?
    val filterShipCommDate = filterCommReceiptDate.filter($"lineitem.l_shipdate" < $"lineitem.l_commitdate")
    val filterReceiptDate = filterShipCommDate.filter($"lineitem.l_commitdate".between("1994-01-01", "1994-12-31")) // SA: l_commitdate -> receiptdate
    val res = filterReceiptDate.groupBy($"lineitem.l_shipmode")
      .agg(sum(when($"o_orderpriority" === "1-URGENT" || $"o_orderpriority" === "2-HIGH", 1).otherwise(0)).alias("high_line_count"),
      sum(when($"o_orderpriority" =!= "1-URGENT" && $"o_orderpriority" =!= "2-HIGH", 1).otherwise(0)).alias("low_line_count"))

    res
  }

  override def getName(): String = "TPCH12"

  override def whyNotQuestion: Twig = {
    var twig = new Twig()
    val root = twig.createNode("root")
    val priority = twig.createNode("l_shipmode", 1, 1, "MAIL")
    val hcount = twig.createNode("high_line_count", 1, 1, "ltltltlt6223")
    val lcount = twig.createNode("low_line_count", 1, 1, "gtgtgtgt9300")
    twig = twig.createEdge(root, priority, false)
    twig = twig.createEdge(root, hcount, false)
    twig = twig.createEdge(root, lcount, false)
    twig.validate.get
  }

  override def computeAlternatives(backtracedWhyNotQuestion: SchemaSubsetTree, input: LeafNode): PrimarySchemaSubsetTree = {
    val primaryTree = super.computeAlternatives(backtracedWhyNotQuestion, input)
    val nestedOrders = input.asInstanceOf[LogicalRelation].relation.asInstanceOf[HadoopFsRelation].location.rootPaths.head.toUri.toString.contains("nestedorders")
    if (!nestedOrders) {
      val saSize = testConfiguration.schemaAlternativeSize
      createAlternatives(primaryTree, saSize)

      for (i <- 0 until saSize by 2) {
        replaceDate(primaryTree.alternatives(i).rootNode)
      }
    }
    primaryTree
  }

  /*
    TODO: we have l_commitdate in multiple places in the query. Does it affect to change the l_commitdate in other filters?
   */
  def replaceDate(node: SchemaNode): Unit ={
    if (node.name == "l_commitdate" && node.children.isEmpty) {
      node.name = "l_receiptdate"
      node.modified = true
      return
    }
    for (child <- node.children){
      replaceDate(child)
    }
  }

}
