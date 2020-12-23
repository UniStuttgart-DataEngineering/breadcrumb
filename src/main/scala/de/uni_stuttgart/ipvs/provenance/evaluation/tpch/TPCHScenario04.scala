package de.uni_stuttgart.ipvs.provenance.evaluation.tpch

import de.uni_stuttgart.ipvs.provenance.evaluation.TestConfiguration
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.{PrimarySchemaSubsetTree, SchemaNode, SchemaSubsetTree}
import de.uni_stuttgart.ipvs.provenance.why_not_question.Twig
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.functions.{avg, count, explode, max, min, sum, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}

class TPCHScenario04(spark: SparkSession, testConfiguration: TestConfiguration) extends TPCHScenario(spark, testConfiguration) {


  import spark.implicits._

//  val decrease = udf { (x: Double, y: Double) => x * (1 - y) }
//  val increase = udf { (x: Double, y: Double) => x * (1 + y) }

/*
Original result:
+---------------+-----------+
|o_orderpriority|order_count|
+---------------+-----------+
|5-LOW          |27922      |
|3-MEDIUM       |28131      |
|1-URGENT       |28135      |
|4-NOT SPECIFIED|28390      |
|2-HIGH         |28245      |
+---------------+-----------+

Rewrite without SA:
java.lang.NullPointerException
	at de.uni_stuttgart.ipvs.provenance.nested_why_not.WhyNotProvenance$.dataFrameAndProvenanceContext(WhyNotProvenance.scala:62)
	at de.uni_stuttgart.ipvs.provenance.nested_why_not.WhyNotProvenance$.rewrite(WhyNotProvenance.scala:68)

Rewrite with SA + MSR:
java.lang.OutOfMemoryError: Java heap space
(Try to run on the cluster)
*/

  override def referenceScenario: DataFrame = {
    val orders = loadOrder()
    val lineitem = loadLineItem()
    val nestedOrders = loadNestedOrders()

//    // Original query
//    val filterOrdDate = orders.filter($"o_orderdate".between("1997-01-01", "1997-03-31"))
//    val filterLine = lineitem.filter($"l_commitdate" < $"l_receiptdate")
//    val joinOrdLine = filterOrdDate.join(filterLine, $"o_orderkey" === $"l_orderkey")
//    var res = joinOrdLine.groupBy($"o_orderpriority").agg(count($"o_orderkey").alias("order_count"))

    // New query
//    var res = nestedOrders.agg(min($"o_orderdate"), max($"o_orderdate"))
    val flattenOrd = nestedOrders.withColumn("lineitem", explode($"o_lineitems"))
    val projectFlatOrd = flattenOrd.select($"o_orderkey", $"o_orderpriority", $"o_orderdate",
      $"lineitem.l_commitdate".alias("commitdate"), $"lineitem.l_shipdate".alias("shipdate")) // SA: l_shipdate -> l_receiptdate
    val filterOrderDate = projectFlatOrd.filter($"o_orderdate".between("1997-01-01", "1997-03-31"))
    val filterCommitShipDate = filterOrderDate.filter($"commitdate" < $"shipdate")
    var res = filterCommitShipDate.groupBy($"o_orderpriority").agg(count($"o_orderkey").alias("order_count"))
//    .sort($"o_orderpriority")

    res
  }

  override def getName(): String = "TPCH04"

  override def whyNotQuestion: Twig = {
    var twig = new Twig()
    val root = twig.createNode("root")
    val priority = twig.createNode("o_orderpriority", 1, 1, "1-URGENT")
    val count = twig.createNode("order_count", 1, 1, "gtgtgtgt25000")
    twig = twig.createEdge(root, priority, false)
    twig = twig.createEdge(root, count, false)
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
