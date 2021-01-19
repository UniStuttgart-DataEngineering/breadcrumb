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

Result over sample:
+---------+------------------+---------+---------------+------+-------------+--------------------------------------------------------------------+---------+
|c_custkey|c_name            |c_acctbal|c_phone        |n_name|c_address    |c_comment                                                           |revenue  |
+---------+------------------+---------+---------------+------+-------------+--------------------------------------------------------------------+---------+
|61402    |Customer#000061402|8285.02  |22-693-385-6828|JAPAN |axsukwH3j7bjp|usly alongside of the final accounts. carefully even packages boost;|5781.8499|
+---------+------------------+---------+---------------+------+-------------+--------------------------------------------------------------------+---------+

Result over sample for mq:
+---------+------------------+---------+---------------+------------+---------------------------------+-----------------------------------------------------------------------------------------------------+------------------+
|c_custkey|c_name            |c_acctbal|c_phone        |n_name      |c_address                        |c_comment                                                                                            |revenue           |
+---------+------------------+---------+---------------+------------+---------------------------------+-----------------------------------------------------------------------------------------------------+------------------+
|61402    |Customer#000061402|8285.02  |22-693-385-6828|JAPAN       |axsukwH3j7bjp                    |usly alongside of the final accounts. carefully even packages boost;                                 |5901.0633         |
|78337    |Customer#000078337|-187.69  |32-756-230-2594|RUSSIA      |tq7bDv1M7,6NXqg3 5gl8T           |dolites. fluffily unusual platelets wake alongside                                                   |87266.5527        |
|67852    |Customer#000067852|2158.51  |24-149-120-9458|KENYA       |OL4BqmkEH4                       |. even, unusual instructions wake along the sly, even packages. pac                                  |17071.622399999997|
|77906    |Customer#000077906|5809.04  |18-381-381-8866|INDIA       |XQjE6VESpa                       |onic, pending deposits until the ironic packa                                                        |134205.48820000002|
|24986    |Customer#000024986|6522.73  |12-757-118-7876|BRAZIL      |wyY bYlIsn,Nu7PJysDoZ9           |symptotes. furiously final packages wake slyly furiously regular theodolites. fin                    |21421.286         |
|23264    |Customer#000023264|344.97   |16-202-600-8748|FRANCE      |9zSn3OgEXLdyUS5Gc3Bquz           | express deposits haggle. ironic courts across the furiou                                            |74242.6468        |
|24383    |Customer#000024383|3188.97  |29-936-313-8286|ROMANIA     |F0YY4cpOwETHooAx                 |use slyly. slyly ironic pearls hagg                                                                  |199360.96279999998|
|77434    |Customer#000077434|8276.45  |11-326-783-1780|ARGENTINA   |M50X6OhDWRbineANf 4vCcEaDT I83hMH|ronic accounts alongside of the f                                                                    |170941.12799999997|
|85171    |Customer#000085171|7559.45  |30-997-682-5645|SAUDI ARABIA|a8sabIdwwyD GG                   |fily bold deposits will have to nag regular warthogs! quickly final deposits ar                      |151678.7106       |
|145231   |Customer#000145231|3979.18  |27-310-532-6591|PERU        |34roD5X0GAKbu33CBpG4             |ly across the slyly final platelets. final requests boost carefully regular pinto beans. slyly regula|27619.9605        |
+---------+------------------+---------+---------------+------------+---------------------------------+-----------------------------------------------------------------------------------------------------+------------------+

Explanation over sample:
+------------------+---------------+-----------+
|pickyOperators    |compatibleCount|alternative|
+------------------+---------------+-----------+
|[0007, 0009]      |1              |000035     |
|[0002, 0007, 0009]|1              |000036     |
+------------------+---------------+-----------+
*/

  def unmodifiedReferenceScenario: DataFrame = {
    val lineitem = loadLineItem001()
    val order = loadOrder001()
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

    val filterLine = lineitem.filter($"l_returnflag" === "X") // oid = 9
    val filterOrd = order.filter($"o_orderdate".between("1992-10-01", "1992-12-31")) // oid = 7
    val joinCustOrd = customer.join(filterOrd, $"c_custkey" === $"o_custkey")
    val joinOrdLine = joinCustOrd.join(filterLine, $"o_orderkey" === $"l_orderkey")
    val joinNation = joinOrdLine.join(nation, $"c_nationkey" === $"n_nationkey")
    val projectExpr = joinNation.withColumn("disc_price", ($"l_extendedprice" * (lit(1.0) - $"l_tax"))) //SA: l_tax -> l_discount
    var res = projectExpr.groupBy($"c_custkey",  $"c_name", $"c_acctbal", $"c_phone", $"n_name", $"c_address", $"c_comment")
      .agg(sum($"disc_price").alias("revenue"))
//    res.filter($"c_custkey" === 57040)
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
    val res = projectExpr.filter($"c_custkey" === 61402)
    res.select($"o_orderdate").distinct().sort($"o_orderdate")
  }

  override def referenceScenario: DataFrame = {
//    return unmodifiedReferenceScenario
    return flatScenarioWithTaxToDiscount
//    return findCustomer
  }

  override def getName(): String = "TPCH10"

  override def whyNotQuestion: Twig = {
    var twig = new Twig()
    val root = twig.createNode("root")
//    val custkey = twig.createNode("c_custkey", 1, 1, "57040")
//    val revenue = twig.createNode("revenue", 1, 1, "ltltltlt740000")
    val custkey = twig.createNode("c_custkey", 1, 1, "61402")
//    val revenue = twig.createNode("revenue", 1, 1, "ltltltlt5900")
    twig = twig.createEdge(root, custkey, false)
//    twig = twig.createEdge(root, revenue, false)
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
//      LineItemAlternatives().createAlternativesWith2Permutations(primaryTree, Seq("l_discount", "l_tax"))

//      val saSize = testConfiguration.schemaAlternativeSize
      val saSize = 1
      createAlternatives(primaryTree, saSize)

      for (i <- 0 until saSize) {
        replaceDate(primaryTree.alternatives(i).rootNode)
      }
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
