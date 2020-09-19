package de.uni_stuttgart.ipvs.provenance.evaluation.crime

import de.uni_stuttgart.ipvs.provenance.evaluation.TestConfiguration
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.{PrimarySchemaSubsetTree, SchemaNode, SchemaSubsetTree}
import de.uni_stuttgart.ipvs.provenance.why_not_question.Twig
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.{DataFrame, SparkSession}

class CrimeScenario2(spark: SparkSession, testConfiguration: TestConfiguration) extends CrimeScenario (spark, testConfiguration) {
  override def getName: String = "C2"

  import spark.implicits._

  override def referenceScenario: DataFrame = {
    val crime = loadCrime()
    val person = loadPerson()
    val sawperson = loadSawperson()
    val witness = loadWitness()

    var res = crime.filter($"csector" > 97)
    val filteredWitness1 = witness.filter($"wname" === "Susan")
    val filteredWitness2 = filteredWitness1.filter($"wsector" > 90)
    res = res.join(filteredWitness2, $"csector" === $"wsector")
    res = res.join(sawperson, $"wname" === $"spwitness")
    res = res.join(person, $"sphair" === $"phair" && $"spclothes" === $"pclothes")
    res = res.select($"pname")
    res
  }

  override def whyNotQuestion: Twig = {
    var twig = new Twig()
    val root = twig.createNode("root")
    val text = twig.createNode("pname", 1, 1, "containsConedera")
    twig = twig.createEdge(root, text, false)
    twig.validate.get
  }

  override def computeAlternatives(backtracedWhyNotQuestion: SchemaSubsetTree, input: LeafNode): PrimarySchemaSubsetTree = {
    val primaryTree = super.computeAlternatives(backtracedWhyNotQuestion, input)
//    val saSize = testConfiguration.schemaAlternativeSize
//    createAlternatives(primaryTree, saSize)

//    for (i <- 0 until saSize by 2) {
//      replace1(primaryTree.alternatives(i).rootNode)
//    }

    primaryTree
  }

  def replace1(node: SchemaNode): Unit ={
    if (node.name == "shair") {
//      node.name = "chair"
      return
    }
    for (child <- node.children){
      replace1(child)
    }
  }
}
