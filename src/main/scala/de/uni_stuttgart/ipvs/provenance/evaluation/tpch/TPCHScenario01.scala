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

  //This query reports the amount of business that was billed, shipped, and returned.
  //Alternatives: 1) l_tax -> l_discount, l_discount -> l_tax
  //              2) shipdate -> commitdate -> receiptdate
  //WN-Question: Why is a value not bigger than xyz
  //Assumed possible answers: filter, aggregation (through alternative), combination of them? --> to be discussed, can be pruned??????
  //Compared to other solutions: aggregation

  val decrease = udf { (x: Double, y: Double) => x * (1 - y) }
  val increase = udf { (x: Double, y: Double) => x * (1 + y) }

  def referenceScenarioOriginal: DataFrame = {
     loadLineItem()
       .filter($"l_shipdate" <= "1998-09-02")
       .groupBy($"l_returnflag", $"l_linestatus")
       .agg(sum($"l_quantity"), sum($"l_extendedprice"),
         sum(decrease($"l_extendedprice", $"l_discount")),
         sum(increase(decrease($"l_extendedprice", $"l_discount"), $"l_tax")),
         avg($"l_quantity"), avg($"l_extendedprice"), avg($"l_discount"), count($"l_quantity"))
       .sort($"l_returnflag", $"l_linestatus")
  }

//  val decrease = udf { (x: Double, y: Double) => x * (1 - y) }
//  val increase = udf { (x: Double, y: Double) => x * (1 + y) }

/*
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

Rewrite without SA:
java.lang.NullPointerException
	at de.uni_stuttgart.ipvs.provenance.nested_why_not.WhyNotProvenance$.dataFrameAndProvenanceContext(WhyNotProvenance.scala:62)
	at de.uni_stuttgart.ipvs.provenance.nested_why_not.WhyNotProvenance$.rewrite(WhyNotProvenance.scala:68)

Rewrite with SA + MSR:
ERROR: org.apache.spark.sql.catalyst.expressions.Multiply cannot be cast to org.apache.spark.sql.catalyst.expressions.NamedExpression
*/

  def nestedScenario: DataFrame = {
//    // Original query
//    val lineitem = loadLineItem()
//    val filterShipDate = lineitem.filter($"l_shipdate".between("1998-06-03", "1998-08-31"))
//    val res = filterShipDate.groupBy($"l_returnflag", $"l_linestatus")
//            .agg(sum($"l_quantity").alias("sum_qty"), sum($"l_extendedprice").alias("sum_base_price"),
//            sum(expr("l_extendedprice * (1 - l_discount)")).alias("sum_disc_price"),
//            sum(expr("l_extendedprice * (1 - l_discount) * (1 + l_tax)")).alias("sum_charge"),
//            avg($"l_quantity").alias("avg_qty"), avg($"l_extendedprice").alias("avg_price"),
//            avg($"l_discount").alias("avg_disc"), count($"l_orderkey").alias("count_order"))
////          .sort($"l_returnflag", $"l_linestatus")

    // New query
    val nestedOrders = loadNestedOrders001()
    val flattened_no = nestedOrders.withColumn("lineitem", explode($"o_lineitems"))
    val project_fno = flattened_no.select($"lineitem.l_returnflag".alias("l_returnflag"), $"lineitem.l_linestatus".alias("l_linestatus"),
                      $"lineitem.l_quantity".alias("l_quantity"), $"lineitem.l_extendedprice".alias("l_extendedprice"),
                      $"lineitem.l_discount".alias("l_discount"), $"lineitem.l_tax".alias("l_tax"),
                      $"lineitem.l_commitdate".alias("l_shipdate"), $"lineitem.l_orderkey".alias("l_orderkey")) // SA: l_commitdate -> l_shipdate
    val project_disc = project_fno.withColumn("disc_price", ($"l_extendedprice"*(lit(1.0)-$"l_discount")))
    val project_charge = project_disc.withColumn("charge", $"l_extendedprice"*(lit(1.0)-$"l_discount")*(lit(1.0)+$"l_tax"))
    val filter_pfno = project_charge.filter($"l_shipdate" <= "1998-09-02")
    val res = filter_pfno.groupBy($"l_returnflag", $"l_linestatus")
        .agg(sum($"l_quantity").alias("sum_qty"),
        sum($"l_extendedprice").alias("sum_base_price"),
//        sum(decrease($"l_extendedprice", $"l_discount")).alias("sum_disc_price"),
//        sum(increase(decrease($"l_extendedprice", $"l_discount"), $"l_tax")).alias("sum_charge"),
        sum($"disc_price").alias("sum_disc_price"),
        sum($"charge").alias("sum_charge"),
        avg($"l_quantity").alias("avg_qty"),
        avg($"l_extendedprice").alias("avg_price"),
        avg($"l_discount").alias("avg_disc"),
        count($"l_orderkey").alias("count_order"))
//      .sort($"returnflag", $"linestatus")

    res
  }

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
        avg($"l_tax").as("AVG_DISC"),
        count($"l_quantity").as("COUNT_ORDER")
      )
  }


  override def referenceScenario: DataFrame = {
    //return minimalScenario
//    return unmodifiedReferenceScenario
//    return scenarioWithTaxAndDiscountInterchanged
//    return scenarioWithTaxAndDiscountInterchangedSmall
    return nestedScenario
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
    //return primaryTree
//    val nestedOrders = input.asInstanceOf[LogicalRelation].relation.asInstanceOf[HadoopFsRelation].location.rootPaths.head.toUri.toString.contains("nestedorders")
//    if (!nestedOrders) {
      val saSize = testConfiguration.schemaAlternativeSize
      createAlternatives(primaryTree, saSize)

      for (i <- 0 until saSize) {
        replaceDiscount1(primaryTree.alternatives(i).rootNode)
        replaceTax(primaryTree.alternatives(i).rootNode)
        replaceDiscount2(primaryTree.alternatives(i).rootNode)
      }
//      for (i <- 0 until saSize by 2) {
//        replaceDate(primaryTree.alternatives(i).rootNode)
//      }
//    }
    primaryTree
  }

  def replaceDiscount1(node: SchemaNode): Unit ={
    if (node.name == "l_discount" && node.children.isEmpty) {
      node.name = "l_discount_tmp"
      node.modified = true
      return
    }
    for (child <- node.children){
        replaceDiscount1(child)
    }
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

  def replaceDiscount2(node: SchemaNode): Unit ={
    if (node.name == "l_discount_tmp" && node.children.isEmpty) {
      node.name = "l_tax"
      node.modified = true
      return
    }
    for (child <- node.children){
      replaceDiscount2(child)
    }
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
