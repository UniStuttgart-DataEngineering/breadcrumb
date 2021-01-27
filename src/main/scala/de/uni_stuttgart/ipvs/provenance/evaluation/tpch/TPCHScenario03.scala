package de.uni_stuttgart.ipvs.provenance.evaluation.tpch

import de.uni_stuttgart.ipvs.provenance.evaluation.TestConfiguration
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.{PrimarySchemaSubsetTree, SchemaNode, SchemaSubsetTree}
import de.uni_stuttgart.ipvs.provenance.why_not_question.Twig
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.functions.{count, countDistinct, explode, expr, lit, sum, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}

class TPCHScenario03(spark: SparkSession, testConfiguration: TestConfiguration) extends TPCHScenario(spark, testConfiguration) {


  import spark.implicits._

//  val decrease = udf { (x: Double, y: Double) => x * (1 - y) }
//  val increase = udf { (x: Double, y: Double) => x * (1 + y) }

/*
Original result for a specific orderkey:
+----------+-----------+--------------+-----------------+
|l_orderkey|o_orderdate|o_shippriority|revenue          |
+----------+-----------+--------------+-----------------+
|4986467   |1994-12-21 |0             |7309.665599999999| -> disappear
+----------+-----------+--------------+-----------------+

Explanations:
+--------------+---------------+-----------+
|pickyOperators|compatibleCount|alternative|
+--------------+---------------+-----------+
|[0009]        |1              |000035     |
|[0002, 0009]  |1              |000042     |
|[0002, 0009]  |1              |000037     |
|[0002, 0009]  |1              |000046     |
|[0002, 0009]  |1              |000038     |
|[0002, 0009]  |1              |000041     |
|[0002, 0009]  |1              |000045     |
|[0002, 0009]  |1              |000044     |
|[0002, 0009]  |1              |000039     |
|[0002, 0009]  |1              |000043     |
|[0002, 0009]  |1              |000040     |
|[0002, 0009]  |1              |000036     |
+--------------+---------------+-----------+

000036: (o_shippriority, l_discount, l_shipdate)
000042: (o_orderpriority,  l_discount, l_shipdate)
a tuple containing follow fragment for 000042:
c_mktsegment = BUILDING,
o_orderdate = 1994-12-21,
1995-02-06 <= l_commitdate <= 1995-03-12,
1994-12-27 <= l_receiptdate <= 1995-04-28
1994-12-24 <= l_shipdate <= 1995-03-31,
o_orderpriority = 3-MEDIUM
*/


  def unmodifiedReferenceScenario: DataFrame = {
    val customer = loadCustomer()
    val orders = loadOrder()
    val lineitem = loadLineItem()

    val filterMktSeg = customer.filter($"c_mktsegment" === "BUILDING")
    val filterOrdDate = orders.filter($"o_orderdate" < "1995-03-15")
    val filterShipDate = lineitem.filter($"l_shipdate" > "1995-03-15")
    val joinCustOrd = filterMktSeg.join(filterOrdDate, $"c_custkey" === $"o_custkey")
    val joinOrdLine = joinCustOrd.join(filterShipDate, $"o_orderkey" === $"l_orderkey")
    val projectExpr = joinOrdLine.withColumn("disc_price", ($"l_extendedprice" * (lit(1.0) - $"l_discount")))
    val res = projectExpr.groupBy($"l_orderkey", $"o_orderdate", $"o_shippriority").agg(sum($"disc_price").alias("revenue"))
//    res.filter($"l_orderkey" === 1468993 || $"l_orderkey" === 4016674 || $"l_orderkey" === 2456423)
    res.filter($"l_orderkey" === 4986467 || $"l_orderkey" == 1468993)
//    res
  }

  def flatScenarioWithCommitToShipDate: DataFrame = {
    val customer = loadCustomer()
    val orders = loadOrder()
    val lineitem = loadLineItem()

    val filterMktSeg = customer.filter($"c_mktsegment" === "BUILDING")
    val filterOrdDate = orders.filter($"o_orderdate" < "1995-03-15")
    val filterShipDate = lineitem.filter($"l_commitdate" > "1995-03-15") // SA: l_commitdate -> l_shipdate // oid = 9
    val joinCustOrd = filterMktSeg.join(filterOrdDate, $"c_custkey" === $"o_custkey")
    val joinOrdLine = joinCustOrd.join(filterShipDate, $"o_orderkey" === $"l_orderkey")
    val projectExpr = joinOrdLine.withColumn("disc_price", ($"l_extendedprice" * (lit(1.0) - $"l_discount")))
    val res = projectExpr.groupBy($"l_orderkey", $"o_orderdate", $"o_shippriority").agg(sum($"disc_price").alias("revenue"))
//    res.filter($"l_orderkey" === 1468993 || $"l_orderkey" === 4016674 || $"l_orderkey" === 2456423)
//    res.filter($"l_orderkey" === 4986467 || $"l_orderkey" == 1468993)
    res
  }

  def flatScenarioWithCommitToShipDateWithSmall: DataFrame = {
    val customer = loadCustomer()
    val orders = loadOrder001()
    val lineitem = loadLineItem001()

    val filterMktSeg = customer.filter($"c_mktsegment" === "BUILDING")
    val filterOrdDate = orders.filter($"o_orderdate" < "1995-03-15")
    val filterShipDate = lineitem.filter($"l_commitdate" > "1995-03-15") // SA: l_commitdate -> l_shipdate // oid = 9
    val joinCustOrd = filterMktSeg.join(filterOrdDate, $"c_custkey" === $"o_custkey")
    val joinOrdLine = joinCustOrd.join(filterShipDate, $"o_orderkey" === $"l_orderkey")
    val projectExpr = joinOrdLine.withColumn("disc_price", ($"l_extendedprice" * (lit(1.0) - $"l_discount")))
    val res = projectExpr.groupBy($"l_orderkey", $"o_orderdate", $"o_shippriority").agg(sum($"disc_price").alias("revenue"))
    //    res.filter($"l_orderkey" === 1468993 || $"l_orderkey" === 4016674 || $"l_orderkey" === 2456423)
//    res.filter($"l_orderkey" === 4986467 || $"l_orderkey" == 1468993)
    res
  }

  def orderKey: DataFrame = {
    val customer = loadCustomer()
    val orders = loadOrder001()
    val lineitem = loadLineItem001()

//    val filterMktSeg = customer.filter($"c_mktsegment" === "BUILDING")
//    val filterOrdDate = orders.filter($"o_orderdate" < "1995-03-15")
//    val filterShipDate = lineitem.filter($"l_commitdate" > "1995-03-15")
    val joinCustOrd = customer.join(orders, $"c_custkey" === $"o_custkey")
    val joinOrdLine = joinCustOrd.join(lineitem, $"o_orderkey" === $"l_orderkey")
    val res = joinOrdLine.filter($"o_orderkey" === 4986467)
    res
  }

  override def referenceScenario: DataFrame = {
//    return unmodifiedReferenceScenario
    return flatScenarioWithCommitToShipDate
//    return flatScenarioWithCommitToShipDateWithSmall
//    return orderKey
  }

  override def getName(): String = "TPCH03"

  override def whyNotQuestion: Twig =   {
    var twig = new Twig()
    val root = twig.createNode("root")
//    val key = twig.createNode("l_orderkey", 1, 1, "1468993") // outdated
//    val rev = twig.createNode("revenue", 1, 1, "ltltltlt9000")
    val key = twig.createNode("l_orderkey", 1, 1, "4986467")
////    val rev = twig.createNode("revenue", 1, 1, "ltltltlt200000")
    twig = twig.createEdge(root, key, false)
//    twig = twig.createEdge(root, rev, false)
    twig.validate.get
  }

  override def computeAlternatives(backtracedWhyNotQuestion: SchemaSubsetTree, input: LeafNode): PrimarySchemaSubsetTree =  {
    val primaryTree = super.computeAlternatives(backtracedWhyNotQuestion, input)
    val lineitem = input.asInstanceOf[LogicalRelation].relation.asInstanceOf[HadoopFsRelation].location.rootPaths.head.toUri.toString.contains("lineitem")
    val order = input.asInstanceOf[LogicalRelation].relation.asInstanceOf[HadoopFsRelation].location.rootPaths.head.toUri.toString.contains("orders")

    if(order) {
      OrdersAlternatives.createAllAlternatives(primaryTree)
    }

    if(lineitem) {
      LineItemAlternatives().createAlternativesWith1Permutations(primaryTree, Seq("l_discount", "l_tax"), Seq("l_commitdate", "l_shipdate", "l_receiptdate"))

////      val saSize = testConfiguration.schemaAlternativeSize
//      val saSize = 1
//      createAlternatives(primaryTree, saSize)
//
//      for (i <- 0 until saSize) {
//        replaceDate(primaryTree.alternatives(i).rootNode)
//      }
    }

    primaryTree
  }

//  def replaceDate(node: SchemaNode): Unit ={
//    if (node.name == "l_commitdate" && node.children.isEmpty) {
//      node.name = "l_shipdate"
//      node.modified = true
//      return
//    }
//    for (child <- node.children){
//      replaceDate(child)
//    }
//  }

}
