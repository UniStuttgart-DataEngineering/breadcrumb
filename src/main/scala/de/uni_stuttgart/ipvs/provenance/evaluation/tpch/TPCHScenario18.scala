package de.uni_stuttgart.ipvs.provenance.evaluation.tpch

import de.uni_stuttgart.ipvs.provenance.evaluation.TestConfiguration
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.{PrimarySchemaSubsetTree, SchemaNode, SchemaSubsetTree}
import de.uni_stuttgart.ipvs.provenance.why_not_question.Twig
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.functions.{explode, lit, sum}
import org.apache.spark.sql.{DataFrame, SparkSession}

class TPCHScenario18(spark: SparkSession, testConfiguration: TestConfiguration) extends TPCHScenario(spark, testConfiguration) {


  import spark.implicits._

//  val decrease = udf { (x: Double, y: Double) => x * (1 - y) }
//  val increase = udf { (x: Double, y: Double) => x * (1 + y) }

/*
TODO: (1) Filter over aggregate
      (2) No meaningful SA exists
Original result for a specific orderkey:
+------------------+---------+----------+-----------+------------+---------------+
|c_name            |c_custkey|o_orderkey|o_orderdate|o_totalprice|sum(l_quantity)|
+------------------+---------+----------+-----------+------------+---------------+
|Customer#000128120|128120   |4722021   |1994-04-07 |544089.09   |323.0          |
+------------------+---------+----------+-----------+------------+---------------+

over sample:

Explanations:

*/

  override def referenceScenario: DataFrame = {
    return unmodifiedReferenceScenario
//    return flatScenarioWithCommitToShipDate
//    return unmodifiedNestedReferenceScenario
//    return nestedScenarioWithCommitToShipDate
  }

  def unmodifiedReferenceScenario: DataFrame = {
    val customer = loadCustomer()
    val orders = loadOrder()
    val lineitem = loadLineItem()

    // Original query
    val inner = lineitem.groupBy($"l_orderkey").agg(sum($"l_quantity").as("sum_quantity"))
    val innerFilter = inner.filter($"sum_quantity" > 300)
    val innerProj = innerFilter.select($"l_orderkey".as("key"), $"sum_quantity")
    val joinOrder = innerProj.join(orders,$"o_orderkey" === $"key")
    val joinLineItem = joinOrder.join(lineitem,$"o_orderkey" === $"l_orderkey")
    val joinCustomer = joinLineItem.join(customer, $"c_custkey" === $"o_custkey")
    val projAttrs = joinCustomer.select($"l_quantity", $"c_name", $"c_custkey", $"o_orderkey", $"o_orderdate", $"o_totalprice")
    val res = projAttrs.groupBy($"c_name", $"c_custkey", $"o_orderkey", $"o_orderdate", $"o_totalprice").agg(sum("l_quantity"))
//      .sort($"o_totalprice".desc, $"o_orderdate")
    res.filter($"c_name" === "Customer#000128120")
//    res
  }

//  def flatScenarioWithCommitToShipDate: DataFrame = {
//    val customer = loadCustomer()
//    val orders = loadOrder()
//    val lineitem = loadLineItem()
//
//    val inner = lineitem.groupBy($"l_orderkey").agg(sum($"l_quantity").as("sum_quantity"))
//    val innerFilter = inner.filter($"sum_quantity" > 300)
//    val innerProj = innerFilter.select($"l_orderkey".as("key"), $"sum_quantity")
//    val joinOrder = innerProj.join(orders,$"o_orderkey" === $"key")
//    val joinLineItem = joinOrder.join(lineitem,$"o_orderkey" === $"l_orderkey")
//    val joinCustomer = joinLineItem.join(customer, $"c_custkey" === $"o_custkey")
//    val projAttrs = joinCustomer.select($"l_quantity", $"c_name", $"c_custkey", $"o_orderkey", $"o_orderdate", $"o_totalprice")
//    val res = projAttrs.groupBy($"c_name", $"c_custkey", $"o_orderkey", $"o_orderdate", $"o_totalprice").agg(sum("l_quantity"))
//    //      .sort($"o_totalprice".desc, $"o_orderdate")
//    res.filter($"c_name" === "Customer#000128120")
////    res
//  }
//
//  def unmodifiedNestedReferenceScenario: DataFrame = {
//    val nestedCustomer = loadNestedCustomer()
//
//
//  }
//
//  def nestedScenarioWithCommitToShipDate: DataFrame = {
//    val nestedCustomer = loadNestedCustomer()
//
//  }


  override def getName(): String = "TPCH18"

  override def whyNotQuestion: Twig =   {
    var twig = new Twig()
    val root = twig.createNode("root")
//    val key = twig.createNode("l_orderkey", 1, 1, "1468993")
//    val rev = twig.createNode("revenue", 1, 1, "ltltltlt9000")
    // Only for sample data
    val key = twig.createNode("l_orderkey", 1, 1, "4986467")
//    val rev = twig.createNode("revenue", 1, 1, "ltltltlt200000")
    twig = twig.createEdge(root, key, false)
//    twig = twig.createEdge(root, rev, false)
    twig.validate.get
  }

  override def computeAlternatives(backtracedWhyNotQuestion: SchemaSubsetTree, input: LeafNode): PrimarySchemaSubsetTree =  {
    val primaryTree = super.computeAlternatives(backtracedWhyNotQuestion, input)
    val saSize = testConfiguration.schemaAlternativeSize
    createAlternatives(primaryTree, saSize)

    for (i <- 0 until saSize) {
      replaceDate(primaryTree.alternatives(i).rootNode)
    }

    primaryTree
  }

  def replaceDate(node: SchemaNode): Unit ={
    if (node.name == "l_commitdate" && node.children.isEmpty) {
      node.name = "l_shipdate"
      node.modified = true
      return
    }
    for (child <- node.children){
      replaceDate(child)
    }
  }
}
