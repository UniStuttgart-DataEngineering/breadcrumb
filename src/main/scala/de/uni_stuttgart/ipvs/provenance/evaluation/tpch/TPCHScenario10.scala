package de.uni_stuttgart.ipvs.provenance.evaluation.tpch

import de.uni_stuttgart.ipvs.provenance.evaluation.TestConfiguration
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.{PrimarySchemaSubsetTree, SchemaNode, SchemaSubsetTree}
import de.uni_stuttgart.ipvs.provenance.why_not_question.Twig
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class TPCHScenario10(spark: SparkSession, testConfiguration: TestConfiguration) extends TPCHScenario(spark, testConfiguration) {


  import spark.implicits._

//  val decrease = udf { (x: Double, y: Double) => x * (1 - y) }
//  val increase = udf { (x: Double, y: Double) => x * (1 + y) }

/*
Original result:
+---------+------------------+---------+---------------+------+----------+----------------------------------------------------------------+-----------------+
|c_custkey|c_name            |c_acctbal|c_phone        |n_name|c_address |c_comment                                                       |revenue          |
+---------+------------------+---------+---------------+------+----------+----------------------------------------------------------------+-----------------+
|57040    |Customer#000057040|632.87   |22-895-641-3466|JAPAN |Eioyzjf4pp|sits. slyly regular requests sleep alongside of the regular inst|734235.2455000001|
+---------+------------------+---------+---------------+------+----------+----------------------------------------------------------------+-----------------+

*/

  def unmodifiedReferenceScenario: DataFrame = {
    val lineitem = loadLineItem()
    val order = loadOrder()
    val customer = loadCustomer()
    val nation = loadNation()

    val filterLine = lineitem.filter($"l_returnflag" === "R")
    val filterOrd = order.filter($"o_orderdate".between("1993-10-01", "1993-12-31"))
    val joinCustOrd = customer.join(filterOrd, $"c_custkey" === $"o_custkey")
    val joinOrdLine = joinCustOrd.join(filterLine, $"o_orderkey" === $"l_orderkey")
    val joinNation = joinOrdLine.join(nation, $"c_nationkey" === $"n_nationkey")
    val projectExpr = joinNation.withColumn("disc_price", ($"l_extendedprice" * (lit(1.0) - $"l_discount")))
    var res = projectExpr.groupBy($"c_custkey",  $"c_name", $"c_acctbal", $"c_phone", $"n_name", $"c_address", $"c_comment")
      .agg(sum($"disc_price").alias("revenue"))
//    res.filter($"c_custkey" === 57040)
    res
  }

  def flatScenarioWithLinestatusToReturnflag: DataFrame = {
    val lineitem = loadLineItem()
    val order = loadOrder()
    val customer = loadCustomer()
    val nation = loadNation()

    val filterLine = lineitem.filter($"l_returnflag" === "R")
    val filterOrd = order.filter($"o_orderdate".between("1993-10-01", "1993-12-31"))
    val joinCustOrd = customer.join(filterOrd, $"c_custkey" === $"o_custkey")
    val joinOrdLine = joinCustOrd.join(filterLine, $"o_orderkey" === $"l_orderkey")
    val joinNation = joinOrdLine.join(nation, $"c_nationkey" === $"n_nationkey")
    val projectExpr = joinNation.withColumn("disc_price", ($"l_extendedprice" * (lit(1.0) - $"l_tax"))) //SA: l_tax -> l_discount
    var res = projectExpr.groupBy($"c_custkey",  $"c_name", $"c_acctbal", $"c_phone", $"n_name", $"c_address", $"c_comment")
      .agg(sum($"disc_price").alias("revenue"))
//    res.filter($"c_custkey" === 57040)
    res
  }


  override def referenceScenario: DataFrame = {
//    return unmodifiedReferenceScenario
    return flatScenarioWithLinestatusToReturnflag
  }

  override def getName(): String = "TPCH10"

  override def whyNotQuestion: Twig = {
    var twig = new Twig()
    val root = twig.createNode("root")
    val custkey = twig.createNode("c_custkey", 1, 1, "57040")
    val revenue = twig.createNode("revenue", 1, 1, "ltltltlt740000")
    twig = twig.createEdge(root, custkey, false)
    twig = twig.createEdge(root, revenue, false)
    twig.validate.get
  }

  override def computeAlternatives(backtracedWhyNotQuestion: SchemaSubsetTree, input: LeafNode): PrimarySchemaSubsetTree = {
    val primaryTree = super.computeAlternatives(backtracedWhyNotQuestion, input)
    val saSize = testConfiguration.schemaAlternativeSize
    createAlternatives(primaryTree, saSize)

    for (i <- 0 until saSize) {
      replaceDate(primaryTree.alternatives(i).rootNode)
    }

    primaryTree
  }

  def replaceDate(node: SchemaNode): Unit ={
    if (node.name == "l_tax" && node.children.isEmpty) {
      node.name = "l_discount"
      node.modified = true
      return
    }
    for (child <- node.children){
      replaceDate(child)
    }
  }

}
