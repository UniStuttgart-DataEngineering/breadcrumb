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

Rewrite without SA:
java.lang.NullPointerException was thrown.
java.lang.NullPointerException
	at de.uni_stuttgart.ipvs.provenance.nested_why_not.WhyNotProvenance$.dataFrameAndProvenanceContext(WhyNotProvenance.scala:62)
	at de.uni_stuttgart.ipvs.provenance.nested_why_not.WhyNotProvenance$.rewrite(WhyNotProvenance.scala:68)

Rewrite with SA + MSR:
TODO: no explanation is retured
  - The revenue is computed with ignoring all the conditions even for SA
*/

  override def referenceScenario: DataFrame = {
//        return unmodifiedReferenceScenario
//      return flatScenarioWithTaxAndDiscountInterchanged
//        return unmodifiedNestedReferenceScenario
    return nestedScenarioWithTaxAndDiscountInterchanged
  }

  def unmodifiedReferenceScenario: DataFrame = {
    val lineitem = loadLineItem()

    // Original query
    val filterShipDate = lineitem.filter($"l_shipdate".between("1994-01-01", "1994-12-31"))
    val filterDisc = filterShipDate.filter($"l_discount".between("0.05", "0.07"))
    val filterQty = filterDisc.filter($"l_quantity" < 24)
    val projectExpr = filterQty.withColumn("disc_price", $"l_extendedprice" * $"l_discount")
    val res = projectExpr.agg(sum($"disc_price").alias("revenue"))
    res
  }

  def flatScenarioWithTaxAndDiscountInterchanged: DataFrame = {
    val lineitem = loadLineItem001()

    val filterShipDate = lineitem.filter($"l_shipdate".between("1994-01-01", "1994-12-31"))
    val filterDisc = filterShipDate.filter($"l_tax".between("0.05", "0.07")) // SA: l_tax -> l_discount
    val filterQty = filterDisc.filter($"l_quantity" < 24)
    val projectExpr = filterQty.withColumn("disc_price", $"l_extendedprice" * $"l_discount")
    val res = projectExpr.agg(sum($"disc_price").alias("revenue"))
    res
  }

  def unmodifiedNestedReferenceScenario: DataFrame = {
    val nestedOrders = loadNestedOrders001()

    val flattenOrd = nestedOrders.withColumn("lineitem", explode($"o_lineitems"))
//    val projectOrdLine = flattenOrd.select($"lineitem.l_extendedprice".alias("l_extendedprice"), $"lineitem.l_discount".alias("l_discount"),
//      $"lineitem.l_shipdate".alias("l_shipdate"), $"lineitem.l_quantity".alias("l_quantity"))
    val filterShipDate = flattenOrd.filter($"lineitem.l_shipdate".between("1994-01-01", "1994-12-31"))
    val filterDisc = filterShipDate.filter($"lineitem.l_discount".between("0.05", "0.07"))
    val filterQty = filterDisc.filter($"lineitem.l_quantity" < 24)
    val projectExpr = filterQty.withColumn("disc_price", $"lineitem.l_extendedprice" * $"lineitem.l_discount")
    val res = projectExpr.agg(sum($"disc_price").alias("revenue"))
    res
  }

  def nestedScenarioWithTaxAndDiscountInterchanged: DataFrame = {
    val nestedOrders = loadNestedOrders001()

    val flattenOrd = nestedOrders.withColumn("lineitem", explode($"o_lineitems"))
    // TODO: Adding this projection with l_discount yields same result as flat query, but it is not with l_tax.
    val projectOrdLine = flattenOrd.select($"lineitem.l_extendedprice".alias("l_extendedprice"), $"lineitem.l_tax".alias("l_discount"),
                  $"lineitem.l_shipdate".alias("l_shipdate"), $"lineitem.l_quantity".alias("l_quantity")) // SA: l_tax -> l_discount
    val filterShipDate = projectOrdLine.filter($"l_shipdate".between("1994-01-01", "1994-12-31"))
    val filterDisc = filterShipDate.filter($"l_discount".between("0.05", "0.07"))
    val filterQty = filterDisc.filter($"l_quantity" < 24)
    val projectExpr = filterQty.withColumn("disc_price", $"l_extendedprice" * $"l_discount")
    val res = projectExpr.agg(sum($"disc_price").alias("revenue"))
    res
  }

  override def getName(): String = "TPCH06"

  override def whyNotQuestion: Twig = {
    var twig = new Twig()
    val root = twig.createNode("root")
//    val revenue = twig.createNode("revenue", 1, 1, "ltltltlt1.2400000000000000E8")
    val revenue = twig.createNode("revenue", 1, 1, "ltltltlt120000")
    twig = twig.createEdge(root, revenue, false)
    twig.validate.get
  }

  override def computeAlternatives(backtracedWhyNotQuestion: SchemaSubsetTree, input: LeafNode): PrimarySchemaSubsetTree = {
    val primaryTree = super.computeAlternatives(backtracedWhyNotQuestion, input)
    val saSize = testConfiguration.schemaAlternativeSize
    createAlternatives(primaryTree, saSize)

    for (i <- 0 until saSize by 2) {
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
