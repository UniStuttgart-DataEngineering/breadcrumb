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
+----------+-----------+--------------+-----------+
|l_orderkey|o_orderdate|o_shippriority|revenue    |
+----------+-----------+--------------+-----------+
|4016674   |1995-02-01 |0             |186785.4098|  -> 207398.6434
|2456423   |1995-03-05 |0             |406181.0111|
|1468993   |1994-12-26 |0             |8964.1652  |  -> disappear
+----------+-----------+--------------+-----------+

over sample:
+----------+-----------+--------------+------------------+
|l_orderkey|o_orderdate|o_shippriority|revenue           |
+----------+-----------+--------------+------------------+
|4986467   |1994-12-21 |0             |7309.665599999999 |  -> disappear
|1225089   |1995-02-20 |0             |85808.42079999999 |  -> 103232.2272
|5331399   |1995-01-18 |0             |128197.82250000001|  -> 39946.4
+----------+-----------+--------------+------------------+

Explanations:
+------------------+---------------+-----------+
|pickyOperators    |compatibleCount|alternative|
+------------------+---------------+-----------+
|[0004, 0009]      |1              |000028     |
|[]                |15             |000028     |  --> shouldn't it be 000029?
|[0004, 0005]      |544            |000028     |
|[0004, 0006]      |693            |000028     |
|[0006]            |168            |000028     |
|[0004]            |41             |000028     |
|[0009]            |1              |000028     |
|[0005]            |170            |000028     |
|[0004, 0005, 0007]|555            |000029     |
|[0005, 0007]      |171            |000029     |
|[0006, 0007]      |168            |000029     |
|[0007]            |20             |000029     |
|[0004, 0006, 0007]|693            |000029     |
|[0004, 0007, 0009]|1              |000029     |
|[0007, 0009]      |1              |000029     |
|[0004, 0007]      |52             |000029     |
+------------------+---------------+-----------+
0004 - project
0005 - filter(orderdate)
0006 - filter(shipdate)
0007 - filter(mktsegment)
0009 - aggregate

000028 - l_commitdate
000029 - l_shipdate
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
    res
  }

  def flatScenarioWithCommitToShipDate: DataFrame = {
    val customer = loadCustomer()
    val orders = loadOrder001()
    val lineitem = loadLineItem001()

    val filterMktSeg = customer.filter($"c_mktsegment" === "BUILDING")
    val filterOrdDate = orders.filter($"o_orderdate" < "1995-03-15")
    val filterShipDate = lineitem.filter($"l_commitdate" > "1995-03-15") // SA: l_commitdate -> l_shipdate
    val joinCustOrd = filterMktSeg.join(filterOrdDate, $"c_custkey" === $"o_custkey")
    val joinOrdLine = joinCustOrd.join(filterShipDate, $"o_orderkey" === $"l_orderkey")
    val projectExpr = joinOrdLine.withColumn("disc_price", ($"l_extendedprice" * (lit(1.0) - $"l_discount")))
    val res = projectExpr.groupBy($"l_orderkey", $"o_orderdate", $"o_shippriority").agg(sum($"disc_price").alias("revenue"))
//    res.filter($"l_orderkey" === 1468993 || $"l_orderkey" === 4016674 || $"l_orderkey" === 2456423)
    res
  }

  override def referenceScenario: DataFrame = {
    //    return unmodifiedReferenceScenario
    return flatScenarioWithCommitToShipDate
  }

  override def getName(): String = "TPCH03"

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
    val lineitem = input.asInstanceOf[LogicalRelation].relation.asInstanceOf[HadoopFsRelation].location.rootPaths.head.toUri.toString.contains("lineitem")
    val order = input.asInstanceOf[LogicalRelation].relation.asInstanceOf[HadoopFsRelation].location.rootPaths.head.toUri.toString.contains("orders")

    if(order) {
      OrdersAlternatives.createAllAlternatives(primaryTree)
    }

    if(lineitem) {
      val saSize = testConfiguration.schemaAlternativeSize
      createAlternatives(primaryTree, saSize)

      for (i <- 0 until saSize) {
        replaceDate(primaryTree.alternatives(i).rootNode)
      }
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
