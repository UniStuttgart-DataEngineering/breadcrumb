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

  override def referenceScenario: DataFrame = {
    val part = loadPart()
    val supplier = loadSupplier()
    val partsupp = loadPartSupp()
    val nation = loadNation()
    val region = loadRegion()

    val filterSize = part.filter($"p_size" === 15)
    val filterType = filterSize.filter($"p_type".contains("BRASS"))
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

    res
  }

  override def getName(): String = "TPCH02"

  override def whyNotQuestion: Twig = null

}
