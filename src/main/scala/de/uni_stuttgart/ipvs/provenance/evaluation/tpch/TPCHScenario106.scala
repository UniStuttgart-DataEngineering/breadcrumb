package de.uni_stuttgart.ipvs.provenance.evaluation.tpch

import de.uni_stuttgart.ipvs.provenance.evaluation.TestConfiguration
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.{PrimarySchemaSubsetTree, SchemaNode, SchemaSubsetTree}
import de.uni_stuttgart.ipvs.provenance.why_not_question.Twig
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class TPCHScenario106(spark: SparkSession, testConfiguration: TestConfiguration) extends TPCHScenario(spark, testConfiguration) {


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

Explanation over sample for md:
+------------------+---------------+-----------+
|pickyOperators    |compatibleCount|alternative|
+------------------+---------------+-----------+
|[0004, 0005]      |1              |000020     |
|[0003, 0004, 0005]|1              |000020     |
|[0004]            |1              |000020     |
|[0003, 0004]      |1              |000020     |
|[0004, 0007]      |1              |000020     | --> interesting
+------------------+---------------+-----------+

Explanation over sample for md2:
+------------------------+---------------+-----------+
|pickyOperators          |compatibleCount|alternative|
+------------------------+---------------+-----------+
|[0004, 0005]            |1              |000024     |
|[0002, 0003, 0004, 0005]|1              |000024     |
|[0003, 0004, 0005]      |1              |000024     |
|[0002, 0004, 0005]      |1              |000024     |
|[0003, 0005]            |1              |000024     |
|[0002, 0005]            |1              |000024     |
|[0005]                  |1              |000024     |
|[0002, 0003, 0005]      |1              |000024     |
|[0005, 0007]            |1              |000024     |
|[0004, 0005]            |1              |000022     |
+------------------------+---------------+-----------+
*/

  def unmodifiedNestedReferenceScenario: DataFrame = {
    val nestedOrders = loadNestedOrders()

    val flattenOrd = nestedOrders.withColumn("lineitem", explode($"o_lineitems"))
    val projectOrdLine = flattenOrd.select($"lineitem.l_discount".alias("l_discount"),
      $"lineitem.l_shipdate".alias("l_shipdate"), $"lineitem.l_quantity".alias("l_quantity"),
      ($"lineitem.l_extendedprice" * $"lineitem.l_discount").alias("disc_price"))
    val filterShipDate = projectOrdLine.filter($"l_shipdate".between("1994-01-01", "1994-12-31"))
    val filterDisc = filterShipDate.filter($"l_discount".between("0.05", "0.07"))
    val filterQty = filterDisc.filter($"l_quantity" < 24)
//    val projectExpr = filterQty.withColumn("disc_price", $"lineitem.l_extendedprice" * $"lineitem.l_discount")
    val res = filterQty.agg(sum($"disc_price").alias("revenue"))
    res
  }

  def nestedScenarioWithTaxToDiscount: DataFrame = {
    val nestedOrders = loadNestedOrders()

    val flattenOrd = nestedOrders.withColumn("lineitem", explode($"o_lineitems"))
    val filterShipDate = flattenOrd.filter($"lineitem.l_shipdate".between("1994-01-01", "1994-12-31"))
    val filterDisc = filterShipDate.filter($"lineitem.l_tax".between("0.05", "0.07"))
    val filterQty = filterDisc.filter($"lineitem.l_quantity" < 24)
    val projectExpr = filterQty.withColumn("disc_price", $"lineitem.l_extendedprice" * $"lineitem.l_discount")
    val res = projectExpr.agg(sum($"disc_price").alias("revenue"))
    res
  }

  def nestedScenarioWithTaxToDiscount2: DataFrame = {
    val nestedOrders = loadNestedOrders()

    val flattenOrd = nestedOrders.withColumn("lineitem", explode($"o_lineitems"))
    val projectOrdLine = flattenOrd.select($"lineitem.l_tax".alias("l_discount"), // SA: l_tax -> l_discount
                  $"lineitem.l_shipdate".alias("l_shipdate"), $"lineitem.l_quantity".alias("l_quantity"),
                  ($"lineitem.l_extendedprice" * $"lineitem.l_discount").alias("disc_price"))
    val filterShipDate = projectOrdLine.filter($"l_shipdate".between("1994-01-01", "1994-12-31"))
    val filterDisc = filterShipDate.filter($"l_discount".between("0.05", "0.07"))
    val filterQty = filterDisc.filter($"l_quantity" < 24)
    val res = filterQty.agg(sum($"disc_price").alias("revenue"))
    res
//    nestedOrders.filter(size($"o_lineitems") === 0)
  }

  override def referenceScenario: DataFrame = {
//    return unmodifiedNestedReferenceScenario
//    return nestedScenarioWithTaxToDiscount
    return nestedScenarioWithTaxToDiscount2
  }

  override def getName(): String = "TPCH106"

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
    val nestedOrders = input.asInstanceOf[LogicalRelation].relation.asInstanceOf[HadoopFsRelation].location.rootPaths.head.toUri.toString.contains("nestedorders")

    if(nestedOrders) {
      LineItemAlternatives().createAlternativesWith1Permutations(primaryTree, Seq("l_discount", "l_tax"), Seq("l_shipdate", "l_receiptdate", "l_commitdate"))

//      val saSize = testConfiguration.schemaAlternativeSize
//      createAlternatives(primaryTree, saSize)
//
//      for (i <- 0 until saSize) {
//        replaceDate(primaryTree.alternatives(i).rootNode)
//      }
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
