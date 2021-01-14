package de.uni_stuttgart.ipvs.provenance.evaluation.tpch

import de.uni_stuttgart.ipvs.provenance.evaluation.TestConfiguration
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.{PrimarySchemaSubsetTree, SchemaNode, SchemaSubsetTree}
import de.uni_stuttgart.ipvs.provenance.why_not_question.Twig
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class TPCHScenario02(spark: SparkSession, testConfiguration: TestConfiguration) extends TPCHScenario(spark, testConfiguration) {


  import spark.implicits._

//  val decrease = udf { (x: Double, y: Double) => x * (1 - y) }
//  val increase = udf { (x: Double, y: Double) => x * (1 + y) }

/*
  1) There should be a better way to write subquery.
  2) The query contains selection over aggregation.
 */

  def unmodifiedReferenceScenario: DataFrame = {
    val part = loadPart()
    val supplier = loadSupplier()
    val partsupp = loadPartSupp()
    val nation = loadNation()
    val region = loadRegion()

    val europe = region.filter($"r_name" === "EUROPE")
      .join(nation, $"r_regionkey" === nation("n_regionkey"))
      .join(supplier, $"n_nationkey" === supplier("s_nationkey"))
      .join(partsupp, supplier("s_suppkey") === partsupp("ps_suppkey"))

    val brass = part
      .filter(part("p_size") === 15)
      .filter(part("p_type").contains("BRASS"))
      .join(europe, europe("ps_partkey") === $"p_partkey")

    val minCost = brass.groupBy(brass("ps_partkey"))
      .agg(min("ps_supplycost").as("min"))
      .select($"ps_partkey".as("min_partkey"), $"min")


    brass.join(minCost, $"ps_partkey" === $"min_partkey")
      .filter(brass("ps_supplycost") === minCost("min"))
      .select("s_acctbal", "s_name", "n_name", "p_partkey", "p_mfgr", "s_address", "s_phone", "s_comment")
      //.sort($"s_acctbal".desc, $"n_name", $"s_name", $"p_partkey")
      //.limit(100)
//      .filter($"s_name" === "Supplier#000005359")
  }

  def referenceScenarioModified: DataFrame = {
    val part = loadPart()
    val supplier = loadSupplier()
    val partsupp = loadPartSupp()
    val nation = loadNation()
    val region = loadRegion()

    val filterSize = part.filter($"p_size" === 15)
    val filterType = filterSize.filter($"p_type".contains("BRASS")) //TODO: SA: p_name -> p_type
    val filterRegion = region.filter($"r_name" === "EUROPE")
    val nationJoinRegion = nation.join(filterRegion, $"n_regionkey" === $"r_regionkey")

    val partsuppJoinPart = partsupp.join(filterType, $"ps_partkey" === $"p_partkey")
    val partsuppJoinSupp = partsuppJoinPart.join(supplier, $"ps_suppkey" === $"s_suppkey")
    val suppJoinNationRegion = partsuppJoinSupp.join(nationJoinRegion, $"s_nationkey" === $"n_nationkey")
//    val joinRegion = joinNation.join(filterRegion, $"n_regionkey" === $"r_regionkey")

    // subquery
//    val subJoinPart = partsupp.join(part,$"ps_partkey" === $"p_partkey")
//    val subJoinSupp = subJoinPart.join(supplier, $"ps_suppkey" === $"s_suppkey")
//    val subJoinNation = subJoinSupp.join(nationJoinRegion, $"s_nationkey" === $"n_nationkey")
    val minCost = suppJoinNationRegion.agg(min($"ps_supplycost").alias("minCost"))

    val filterCost = suppJoinNationRegion.join(minCost, $"ps_supplycost" === $"minCost")
    val res = filterCost.select($"s_acctbal", $"s_name", $"n_name", $"p_partkey", $"p_mfgr", $"s_address", $"s_phone", $"s_comment")
//        .sort($"s_acctbal".desc, $"s_name", $"n_name", $"p_partkey")

//    res.filter($"s_name" === "Supplier#000005359")
    res
  }


  override def referenceScenario(): DataFrame = {
//    return unmodifiedReferenceScenario
    return referenceScenarioModified
  }

  override def getName(): String = "TPCH02"

  override def whyNotQuestion: Twig =   {
    var twig = new Twig()
    val root = twig.createNode("root")
    val key = twig.createNode("n_name", 1, 1, "CHINA")
    val rev = twig.createNode("p_partkey", 1, 1, "249")
    twig = twig.createEdge(root, key, false)
    twig = twig.createEdge(root, rev, false)
    twig.validate.get
  }

  override def computeAlternatives(backtracedWhyNotQuestion: SchemaSubsetTree, input: LeafNode): PrimarySchemaSubsetTree = {

    val primaryTree = super.computeAlternatives(backtracedWhyNotQuestion, input)
//    val saSize = testConfiguration.schemaAlternativeSize
//    createAlternatives(primaryTree, saSize)
//
//    for (i <- 0 until saSize) {
//      replaceName(primaryTree.alternatives(i).rootNode)
//    }

    primaryTree
  }

  def replaceName(node: SchemaNode): Unit ={
    if (node.name == "p_name" && node.children.isEmpty) {
      node.name = "p_type"
      node.modified = true
      return
    }
    for (child <- node.children){
      replaceName(child)
    }
  }


}
