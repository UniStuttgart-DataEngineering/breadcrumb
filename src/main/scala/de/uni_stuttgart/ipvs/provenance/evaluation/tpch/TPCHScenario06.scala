package de.uni_stuttgart.ipvs.provenance.evaluation.tpch

import de.uni_stuttgart.ipvs.provenance.evaluation.TestConfiguration
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.{PrimarySchemaSubsetTree, SchemaNode, SchemaSubsetTree}
import de.uni_stuttgart.ipvs.provenance.why_not_question.Twig
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class TPCHScenario06(spark: SparkSession, testConfiguration: TestConfiguration) extends TPCHScenario(spark, testConfiguration) {


  import spark.implicits._

//  val decrease = udf { (x: Double, y: Double) => x * (1 - y) }
//  val increase = udf { (x: Double, y: Double) => x * (1 + y) }

/*
Original result:
+--------------------+
|revenue             |
+--------------------+
|1.2314107822829978E8|
+--------------------+

over sample:
+------------------+
|revenue           |
+------------------+
|115092.73550000002|
+------------------+

Result of modified query over sample:
+------------------+
|revenue           |
+------------------+
|133977.07929999992|
+------------------+

Why-not question:
Why revenue is not less than 120000

Explanation over sample:
+------------------------+---------------+-----------+
|pickyOperators          |compatibleCount|alternative|
+------------------------+---------------+-----------+
|[0002, 0003, 0004, 0005]|1              |000017     |
|[0002, 0004, 0005]      |1              |000017     |
|[0002, 0004]            |1              |000017     |
|[0002, 0003, 0004]      |1              |000017     |
|[0002, 0003, 0004, 0005]|1              |000018     |
|[0002, 0004, 0005]      |1              |000018     |
|[0002, 0003, 0004, 0005]|1              |000019     |
|[0002, 0004, 0005]      |1              |000019     |
+------------------------+---------------+-----------+

Without SA:
+------------------+---------------+-----------+
|pickyOperators    |compatibleCount|alternative|
+------------------+---------------+-----------+
|[0003, 0004]      |1              |000014     |
|[]                |1              |000014     |
|[0003, 0005]      |1              |000014     |
|[0005]            |1              |000014     |
|[0003]            |1              |000014     |
|[0004, 0005]      |1              |000014     |
|[0003, 0004, 0005]|1              |000014     |
|[0004]            |1              |000014     |
+------------------+---------------+-----------+
*/


  def unmodifiedReferenceScenario: DataFrame = {
    val lineitem = loadLineItem()

    val filterShipDate = lineitem.filter($"l_shipdate".between("1994-01-01", "1994-12-31"))
    val filterDisc = filterShipDate.filter($"l_discount".between("0.05", "0.07"))
    val filterQty = filterDisc.filter($"l_quantity" < 24)
    val projectExpr = filterQty.withColumn("disc_price", $"l_extendedprice" * $"l_discount")
    val res = projectExpr.agg(sum($"disc_price").alias("revenue"))
    res
  }

  def flatScenarioWithTaxToDiscount: DataFrame = {
    val lineitem = loadLineItem()

    val filterShipDate = lineitem.filter($"l_shipdate".between("1994-01-01", "1994-12-31"))
    val filterDisc = filterShipDate.filter($"l_tax".between("0.05", "0.07")) // SA: l_tax -> l_discount
    val filterQty = filterDisc.filter($"l_quantity" < 24)
    val projectExpr = filterQty.withColumn("disc_price", $"l_extendedprice" * $"l_discount")
    val res = projectExpr.agg(sum($"disc_price").alias("revenue"))
    res
  }

  def flatScenarioWithTaxToDiscountWithSmall: DataFrame = {
    val lineitem = loadLineItem001()

    val filterShipDate = lineitem.filter($"l_shipdate".between("1994-01-01", "1994-12-31"))
    val filterDisc = filterShipDate.filter($"l_tax".between("0.05", "0.07")) // SA: l_tax -> l_discount
    val filterQty = filterDisc.filter($"l_quantity" < 24)
    val projectExpr = filterQty.withColumn("disc_price", $"l_extendedprice" * $"l_discount")
    val res = projectExpr.agg(sum($"disc_price").alias("revenue"))
    res
  }

  override def referenceScenario: DataFrame = {
//    return unmodifiedReferenceScenario
    return flatScenarioWithTaxToDiscount
//    return flatScenarioWithTaxToDiscountWithSmall
  }

  override def getName(): String = "TPCH06"

  override def whyNotQuestion: Twig = {
    var twig = new Twig()
    val root = twig.createNode("root")
    val revenue = twig.createNode("revenue", 1, 1, "ltltltlt1.2400000000000000E8")
//    val revenue = twig.createNode("revenue", 1, 1, "ltltltlt120000") // for sample data
    twig = twig.createEdge(root, revenue, false)
    twig.validate.get
  }

  override def computeAlternatives(backtracedWhyNotQuestion: SchemaSubsetTree, input: LeafNode): PrimarySchemaSubsetTree = {
    val primaryTree = super.computeAlternatives(backtracedWhyNotQuestion, input)
    val lineitem = input.asInstanceOf[LogicalRelation].relation.asInstanceOf[HadoopFsRelation].location.rootPaths.head.toUri.toString.contains("lineitem")

    if(lineitem) {
      getLineItemAlternatives1(primaryTree, Seq("l_discount", "l_tax"), Seq("l_shipdate", "l_receiptdate", "l_commitdate"))

//      LineItemAlternatives().createAlternativesWith1Permutations(primaryTree, Seq("l_discount", "l_tax"), Seq("l_shipdate", "l_receiptdate", "l_commitdate"))

//      val saSize = testConfiguration.schemaAlternativeSize
//      createAlternatives(primaryTree, saSize)
//
//      for (i <- 0 until saSize) {
//        replaceDate(primaryTree.alternatives(i).rootNode)
//      }
    }

    primaryTree
  }

//  def replaceDate(node: SchemaNode): Unit ={
//    if (node.name == "l_tax" && node.children.isEmpty) {
//      node.name = "l_discount"
//      node.modified = true
//      return
//    }
//    for (child <- node.children){
//      replaceDate(child)
//    }
//  }

}
