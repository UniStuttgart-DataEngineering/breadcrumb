package de.uni_stuttgart.ipvs.provenance.evaluation.tpch

import de.uni_stuttgart.ipvs.provenance.evaluation.TestConfiguration
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.{PrimarySchemaSubsetTree, SchemaNode, SchemaSubsetTree}
import de.uni_stuttgart.ipvs.provenance.why_not_question.Twig
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{avg, count, lit, sum, udf, explode, expr}

class TPCHScenario01(spark: SparkSession, testConfiguration: TestConfiguration) extends TPCHScenario(spark, testConfiguration) {

  import spark.implicits._

//
//  val decrease = udf { (x: Double, y: Double) => x * (1 - y) }
//  val increase = udf { (x: Double, y: Double) => x * (1 + y) }
//
//  def referenceScenarioOriginal: DataFrame = {
//     loadLineItem()
//       .filter($"l_shipdate" <= "1998-09-02")
//       .groupBy($"l_returnflag", $"l_linestatus")
//       .agg(sum($"l_quantity"), sum($"l_extendedprice"),
//         sum(decrease($"l_extendedprice", $"l_discount")),
//         sum(increase(decrease($"l_extendedprice", $"l_discount"), $"l_tax")),
//         avg($"l_quantity"), avg($"l_extendedprice"), avg($"l_discount"), count($"l_quantity"))
//       .sort($"l_returnflag", $"l_linestatus")
//  }

/*
This query reports the amount of business that was billed, shipped, and returned.
Alternatives: 1) l_tax -> l_discount, l_discount -> l_tax
              2) shipdate -> commitdate -> receiptdate
WN-Question: Why is a value not bigger than xyz
Assumed possible answers: filter, aggregation (through alternative), combination of them? --> to be discussed, can be pruned??????
Compared to other solutions: aggregation

Original result:
+----------+----------+-----------+---------------------+---------------------+---------------------+------------------+------------------+--------------------+-----------+
|returnflag|linestatus|sum_qty    |sum_base_price       |sum_disc_price       |sum_charge           |avg_qty           |avg_price         |avg_disc            |count_order|
+----------+----------+-----------+---------------------+---------------------+---------------------+------------------+------------------+--------------------+-----------+
|N         |F         |991417.0   |1.4875047103799994E9 |1.4130821680541003E9 |1.469649223194375E9  |25.516471920522985|38284.46776084829 |0.050093426674216304|38854      |
|A         |F         |3.7734107E7|5.658655440072991E10 |5.3758257134870094E10|5.590906522282768E10 |25.522005853257337|38273.129734621616|0.04998529583841114 |1478493    |
|N         |O         |7.447604E7 |1.1170172969774011E11|1.0611823030760521E11|1.1036704387249742E11|25.50222676958499 |38249.11798890831 |0.04999658605375758 |2920374    |
|R         |F         |3.7719753E7|5.656804138090003E10 |5.374129268460402E10 |5.588961911983207E10 |25.50579361269077 |38250.85462609968 |0.05000940583014063 |1478870    |
+----------+----------+-----------+---------------------+---------------------+---------------------+------------------+------------------+--------------------+-----------+

over sample:
+------------+------------+-------+--------------------+--------------------+--------------------+------------------+------------------+--------------------+-----------+
|l_returnflag|l_linestatus|sum_qty|sum_base_price      |sum_disc_price      |sum_charge          |avg_qty           |avg_price         |avg_disc            |count_order|
+------------+------------+-------+--------------------+--------------------+--------------------+------------------+------------------+--------------------+-----------+
|N           |F           |951.0  |1401255.9500000002  |1340444.3708000001  |1400174.4904700003  |23.195121951219512|34176.97439024391 |0.04121951219512197 |41         |
|A           |F           |39895.0|6.016343477999999E7 |5.710262086479996E7 |5.936668842011303E7 |25.905844155844157|39067.165441558434|0.050396103896103876|1540       |
|N           |O           |84257.0|1.2612064072000009E8|1.1980557708120029E8|1.2458525596504119E8|25.401567681640035|38022.502478142924|0.05023515224600577 |3317       |
|R           |F           |39644.0|5.920035068000007E7 |5.619490972560006E7 |5.845945158448006E7 |25.315453384418902|37803.544495530055|0.05042145593869733 |1566       |
+------------+------------+-------+--------------------+--------------------+--------------------+------------------+------------------+--------------------+-----------+

Result of new query
+------------+------------+-----------+---------------------+---------------------+---------------------+------------------+------------------+--------------------+-----------+
|l_returnflag|l_linestatus|SUM_QTY    |SUM_BASE_PRICE       |SUM_DISC_PRICE       |SUM_CHARGE           |AVG_QTY           |AVG_PRICE         |AVG_DISC            |COUNT_ORDER|
+------------+------------+-----------+---------------------+---------------------+---------------------+------------------+------------------+--------------------+-----------+
|N           |F           |991417.0   |1.4875047103800015E9 |1.4279427833722003E9 |1.4993704538305743E9 |25.516471920522985|38284.46776084835 |0.039976064240490355|38854      |
|A           |F           |3.7734107E7|5.658655440073042E10 |5.4322551501665764E10|5.7037653956419395E10|25.522005853257337|38273.12973462196 |0.03999960770870729 |1478493    |
|N           |O           |7.447604E7 |1.1170172969774013E11|1.0722943502365518E11|1.1258945330459906E11|25.50222676958499 |38249.117988908314|0.04003812867809482 |2920374    |
|R           |F           |3.7719753E7|5.656804138089943E10 |5.4306781776778564E10|5.702059730418154E10 |25.50579361269077 |38250.85462609927 |0.03998597577885089 |1478870    |
+------------+------------+-----------+---------------------+---------------------+---------------------+------------------+------------------+--------------------+-----------+

Explanation:
+--------------+---------------+-----------+
|pickyOperators|compatibleCount|alternative|
+--------------+---------------+-----------+
|[0003, 0004]  |3              |000017     |
|[0003, 0004]  |3              |000016     |
|[0003]        |3              |000015     |
|[0003, 0004]  |1              |000015     |
+--------------+---------------+-----------+
*/

  def unmodifiedReferenceScenario: DataFrame = {
    loadLineItem()
      .filter($"l_shipdate" <= "1998-09-02")
      .withColumn("disc_price", ($"l_extendedprice"*(lit(1.0)-$"l_discount")))
      .withColumn("charge", $"l_extendedprice"*(lit(1.0)-$"l_discount")*(lit(1.0)+$"l_tax"))
      .groupBy($"l_returnflag", $"l_linestatus")
      .agg(
        sum($"l_quantity").as("SUM_QTY"),
        sum($"l_extendedprice").as("SUM_BASE_PRICE"),
        sum($"disc_price").as("SUM_DISC_PRICE"),
        sum($"charge").as("SUM_CHARGE"),
        avg($"l_quantity").as("AVG_QTY"),
        avg($"l_extendedprice").as("AVG_PRICE"),
        avg($"l_discount").as("AVG_DISC"),
        count($"l_quantity").as("COUNT_ORDER")
      )
  }

  def scenarioWithTaxAndDiscountInterchanged: DataFrame = {
    loadLineItem()
      .filter($"l_shipdate" <= "1998-09-02")
      .withColumn("disc_price", ($"l_extendedprice"*(lit(1.0)-$"l_tax")))
      .withColumn("charge", $"l_extendedprice"*(lit(1.0)+$"l_discount")*(lit(1.0)-$"l_tax"))
      .groupBy($"l_returnflag", $"l_linestatus")
      .agg(
        sum($"l_quantity").as("SUM_QTY"),
        sum($"l_extendedprice").as("SUM_BASE_PRICE"),
        sum($"disc_price").as("SUM_DISC_PRICE"),
        sum($"charge").as("SUM_CHARGE"),
        avg($"l_quantity").as("AVG_QTY"),
        avg($"l_extendedprice").as("AVG_PRICE"),
        avg($"l_tax").as("AVG_DISC"),
        count($"l_quantity").as("COUNT_ORDER")
      )
  }


  def scenarioWithTaxAndDiscountInterchangedSmall: DataFrame = {
    loadLineItem001()
      .filter($"l_shipdate" <= "1998-09-02")
      .withColumn("disc_price", ($"l_extendedprice"*(lit(1.0)-$"l_tax")))
      .withColumn("charge", $"l_extendedprice"*(lit(1.0)-$"l_discount")*(lit(1.0)+$"l_tax"))
      .groupBy($"l_returnflag", $"l_linestatus")
      .agg(
        sum($"l_quantity").as("SUM_QTY"),
        sum($"l_extendedprice").as("SUM_BASE_PRICE"),
        sum($"disc_price").as("SUM_DISC_PRICE"),
        sum($"charge").as("SUM_CHARGE"),
        avg($"l_quantity").as("AVG_QTY"),
        avg($"l_extendedprice").as("AVG_PRICE"),
        avg($"l_tax").as("AVG_DISC"),
        count($"l_quantity").as("COUNT_ORDER")
      )
  }

  def minimalScenario: DataFrame = {
    loadLineItem()
      .withColumn("disc_price", ($"l_extendedprice"*(lit(1.0)-$"l_discount")))
      .groupBy($"l_returnflag", $"l_linestatus")
      .agg(
        sum($"l_quantity").as("SUM_QTY")
       ,sum($"disc_price").as("SUM_DISC_PRICE")
      )
  }


  override def referenceScenario: DataFrame = {
//    return minimalScenario
//    return unmodifiedReferenceScenario
    return scenarioWithTaxAndDiscountInterchanged
//    return scenarioWithTaxAndDiscountInterchangedSmall
  }


  override def getName(): String = "TPCH01"

  override def whyNotQuestion: Twig = {
    var twig = new Twig()
    val root = twig.createNode("root")
    val avg_disc = twig.createNode("AVG_DISC", 1, 1, "gtgtgtgt0.045")
    twig = twig.createEdge(root, avg_disc, false)
    twig.validate.get
  }

//  def whyNotQuestion: Twig = {
//    var twig = new Twig()
//    val root = twig.createNode("root")
//    val rf = twig.createNode("l_returnflag", 1, 1, "N")
//    val ls = twig.createNode("l_linestatus", 1, 1, "O")
//    val co = twig.createNode("sum_qty", 1, 1, "ltltltlt77000")
////    val co = twig.createNode("count_order", 1, 1, "ltltltlt3320") // should be * 1000 in the factor 1 dataset
//    twig = twig.createEdge(root, rf, false)
//    twig = twig.createEdge(root, ls, false)
//    twig = twig.createEdge(root, co, false)
//    twig.validate.get
//  }

  override def computeAlternatives(backtracedWhyNotQuestion: SchemaSubsetTree, input: LeafNode): PrimarySchemaSubsetTree = {
    val primaryTree = super.computeAlternatives(backtracedWhyNotQuestion, input)

    //LineItemAlternatives().createAlternatives1(primaryTree, Seq("l_discount", "l_tax"))
    LineItemAlternatives().createAlternativesWith1Permutations(primaryTree, Seq("l_discount", "l_tax"), Seq("l_shipdate", "l_receiptdate", "l_commitdate"))


    /*
    val saSize = testConfiguration.schemaAlternativeSize
    createAlternatives(primaryTree, saSize)

    for (i <- 0 until saSize) {
      replaceDiscount1(primaryTree.alternatives(i).rootNode)
      replaceTax(primaryTree.alternatives(i).rootNode)
      replaceDiscount2(primaryTree.alternatives(i).rootNode)
    } */
//      for (i <- 0 until saSize by 2) {
//        replaceDate(primaryTree.alternatives(i).rootNode)
//      }

    primaryTree
  }

//  def replaceDiscount1(node: SchemaNode): Unit ={
//    if (node.name == "l_discount" && node.children.isEmpty) {
//      node.name = "l_discount_tmp"
//      node.modified = true
//      return
//    }
//    for (child <- node.children){
//        replaceDiscount1(child)
//    }
//  }
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
//
//  def replaceDiscount2(node: SchemaNode): Unit ={
//    if (node.name == "l_discount_tmp" && node.children.isEmpty) {
//      node.name = "l_tax"
//      node.modified = true
//      return
//    }
//    for (child <- node.children){
//      replaceDiscount2(child)
//    }
//  }
//
//  def replaceTax(node: SchemaNode): Unit ={
//    if (node.name == "l_tax" && node.children.isEmpty) {
//      node.name = "l_discount"
//      node.modified = true
//      return
//    }
//    for (child <- node.children){
//      replaceTax(child)
//    }
//  }

}
