package de.uni_stuttgart.ipvs.provenance.evaluation.crime

import de.uni_stuttgart.ipvs.provenance.evaluation.TestConfiguration
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.{PrimarySchemaSubsetTree, SchemaNode, SchemaSubsetTree}
import de.uni_stuttgart.ipvs.provenance.why_not_question.Twig
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.{DataFrame, SparkSession}

class CrimeScenario3(spark: SparkSession, testConfiguration: TestConfiguration) extends CrimeScenario (spark, testConfiguration) {
  override def getName: String = "C3"

  import spark.implicits._

  override def referenceScenario: DataFrame = {
    val crime = loadCrime()
    val sawperson = loadSawperson()
    val witness = loadWitness()
    var res = crime.join(witness, $"csector" === $"wsector")
    res = res.join(sawperson, $"wname" === $"spwitness")
    res = res.select($"wname", $"sphair".alias("characteristic"), $"ctype")
    res
  }

  override def whyNotQuestion: Twig = {
    var twig = new Twig()
    val root = twig.createNode("root")
    val text = twig.createNode("wname", 1, 1, "Ashishbakshi")
    val characteristic = twig.createNode("characteristic", 1, 1, "snow")
    twig = twig.createEdge(root, text, false)
    twig = twig.createEdge(root, characteristic, false)
    twig.validate.get
  }

  override def computeAlternatives(backtracedWhyNotQuestion: SchemaSubsetTree, input: LeafNode): PrimarySchemaSubsetTree = {
    val primaryTree = super.computeAlternatives(backtracedWhyNotQuestion, input)
    //    val saSize = testConfiguration.schemaAlternativeSize
    val sawperson = input.asInstanceOf[LogicalRelation].relation.asInstanceOf[HadoopFsRelation].location.rootPaths.head.toUri.toString.contains("sawperson")
    println(sawperson)
    if (sawperson) {
      createAlternatives(primaryTree, 1)
      replace1(primaryTree.alternatives(0).rootNode)
    }
    primaryTree
  }

  def replace1(node: SchemaNode): Unit ={
    if (node.name == "sphair") {
      node.name = "spclothes"
      node.modified = true
      return
    }
    for (child <- node.children){
      replace1(child)
    }
  }
}
