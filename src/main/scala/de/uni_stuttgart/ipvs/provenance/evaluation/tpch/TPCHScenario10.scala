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
+---------+------------------+---------+---------------+------+-------------+--------------------------------------------------------------------+---------+
|c_custkey|c_name            |c_acctbal|c_phone        |n_name|c_address    |c_comment                                                           |revenue  |
+---------+------------------+---------+---------------+------+-------------+--------------------------------------------------------------------+---------+
|61402    |Customer#000061402|8285.02  |22-693-385-6828|JAPAN |axsukwH3j7bjp|usly alongside of the final accounts. carefully even packages boost;|5781.8499|
+---------+------------------+---------+---------------+------+-------------+--------------------------------------------------------------------+---------+

Result over mq:
+---------+------------------+---------+---------------+------+-------------+--------------------------------------------------------------------+-----------+
|c_custkey|c_name            |c_acctbal|c_phone        |n_name|c_address    |c_comment                                                           |revenue    |
+---------+------------------+---------+---------------+------+-------------+--------------------------------------------------------------------+-----------+
|61402    |Customer#000061402|8285.02  |22-693-385-6828|JAPAN |axsukwH3j7bjp|usly alongside of the final accounts. carefully even packages boost;|560945.1556|
+---------+------------------+---------+---------------+------+-------------+--------------------------------------------------------------------+-----------+


Explanation over sample:
+------------------+---------------+-----------+
|pickyOperators    |compatibleCount|alternative|
+------------------+---------------+-----------+
|[0002]            |1              |000036     |
|[0002, 0007, 0009]|1              |000036     |
|[0002, 0007]      |1              |000036     |
+------------------+---------------+-----------+
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
    res.filter($"c_custkey" === 61402)
//    res
  }

  def flatScenarioWithTaxToDiscount: DataFrame = {
    val lineitem = loadLineItem()
    val order = loadOrder()
    val customer = loadCustomer()
    val nation = loadNation()

    val filterLine = lineitem.filter($"l_returnflag" === "N") // oid = 9
    val filterOrd = order.filter($"o_orderdate".between("1997-10-01", "1997-12-31")) // oid = 7
    val joinCustOrd = customer.join(filterOrd, $"c_custkey" === $"o_custkey")
    val joinOrdLine = joinCustOrd.join(filterLine, $"o_orderkey" === $"l_orderkey")
    val joinNation = joinOrdLine.join(nation, $"c_nationkey" === $"n_nationkey")
    val projectExpr = joinNation.withColumn("disc_price", ($"l_extendedprice" * (lit(1.0) - $"l_tax"))) //SA: l_tax -> l_discount
    var res = projectExpr.groupBy($"c_custkey",  $"c_name", $"c_acctbal", $"c_phone", $"n_name", $"c_address", $"c_comment")
      .agg(sum($"disc_price").alias("revenue"))
//    res.filter($"c_custkey" === 61402)
    res
  }

  def flatScenarioWithTaxToDiscountWithSmall: DataFrame = {
    val lineitem = loadLineItem001()
    val order = loadOrder001()
    val customer = loadCustomer()
    val nation = loadNation()

    val filterLine = lineitem.filter($"l_returnflag" === "N") // oid = 9
//    val filterOrd = order.filter($"o_orderdate".between("1992-10-01", "1992-12-31")) // oid = 7
    val filterOrd = order.filter($"o_orderdate".between("1997-10-01", "1997-12-31")) // oid = 7
    val joinCustOrd = customer.join(filterOrd, $"c_custkey" === $"o_custkey")
    val joinOrdLine = joinCustOrd.join(filterLine, $"o_orderkey" === $"l_orderkey")
    val joinNation = joinOrdLine.join(nation, $"c_nationkey" === $"n_nationkey")
    val projectExpr = joinNation.withColumn("disc_price", ($"l_extendedprice" * (lit(1.0) - $"l_tax"))) //SA: l_tax -> l_discount
    var res = projectExpr.groupBy($"c_custkey",  $"c_name", $"c_acctbal", $"c_phone", $"n_name", $"c_address", $"c_comment")
      .agg(sum($"disc_price").alias("revenue"))
//    res.filter($"c_custkey" === 61402)
    res
  }

  def findCustomer: DataFrame = {
    val lineitem = loadLineItem()
    val order = loadOrder()
    val customer = loadCustomer()
    val nation = loadNation()

//    order.select($"o_orderdate".substr(1,4)).distinct()

//    val filterLine = lineitem.filter($"l_returnflag" === "R") // oid = 9
//    val filterOrd = order.filter($"o_orderdate".between("1993-10-01", "1993-12-31")) // oid = 7
    val joinCustOrd = customer.join(order, $"c_custkey" === $"o_custkey")
    val joinOrdLine = joinCustOrd.join(lineitem, $"o_orderkey" === $"l_orderkey")
    val joinNation = joinOrdLine.join(nation, $"c_nationkey" === $"n_nationkey")
    val projectExpr = joinNation.withColumn("disc_price", ($"l_extendedprice" * (lit(1.0) - $"l_tax"))) //SA: l_tax -> l_discount
    val res = projectExpr.filter($"c_custkey" === 61402 && $"l_returnflag" === "N")
    res.select($"o_orderdate").distinct().sort($"o_orderdate")
//    val res = projectExpr.select($"l_returnflag").distinct()
//    val res = projectExpr.groupBy($"c_custkey").agg(count($"l_returnflag").alias("cntFlag"))
//    res.filter($"cntFlag" < 3)
//    res
  }

  override def referenceScenario: DataFrame = {
//    return unmodifiedReferenceScenario
    return flatScenarioWithTaxToDiscount
//    return flatScenarioWithTaxToDiscountWithSmall
//    return findCustomer
  }

  override def getName(): String = "TPCH10"

  override def whyNotQuestion: Twig = {
    var twig = new Twig()
    val root = twig.createNode("root")
//    val custkey = twig.createNode("c_custkey", 1, 1, "57040") // outdated
//    val revenue = twig.createNode("revenue", 1, 1, "ltltltlt740000") // outdated
    val custkey = twig.createNode("c_custkey", 1, 1, "61402") // for both full and sample data
    val revenue = twig.createNode("revenue", 1, 1, "ltltltlt560000")
    twig = twig.createEdge(root, custkey, false)
    twig = twig.createEdge(root, revenue, false)
    twig.validate.get
  }

  override def computeAlternatives(backtracedWhyNotQuestion: SchemaSubsetTree, input: LeafNode): PrimarySchemaSubsetTree = {
    val primaryTree = super.computeAlternatives(backtracedWhyNotQuestion, input)
    val lineitem = input.asInstanceOf[LogicalRelation].relation.asInstanceOf[HadoopFsRelation].location.rootPaths.head.toUri.toString.contains("lineitem")
//    val order = input.asInstanceOf[LogicalRelation].relation.asInstanceOf[HadoopFsRelation].location.rootPaths.head.toUri.toString.contains("orders")
//
//    if(order) {
//      OrdersAlternatives.createAllAlternatives(primaryTree)
//    }

    if(lineitem) {
      if ((testConfiguration.schemaAlternativeSize & 1) > 0){
        createAlternatives(primaryTree, 1)
        replaceTax(primaryTree.alternatives(1).rootNode)
      }
    }
//      LineItemAlternatives().createAlternativesWith2Permutations(primaryTree, Seq("l_discount", "l_tax"))

//      val saSize = testConfiguration.schemaAlternativeSize
      /* & 1
      val saSize = 1
      createAlternatives(primaryTree, saSize)

      for (i <- 0 until saSize) {
        replaceDate(primaryTree.alternatives(i).rootNode)
      }
    }
    */

    primaryTree
  }

  def replaceTax(node: SchemaNode): Unit ={
    if (node.name == "l_tax" && node.children.isEmpty) {
      node.name = "l_discount"
      node.modified = true
      return
    }
    for (child <- node.children){
      replaceTax(child)
    }
  }

}
