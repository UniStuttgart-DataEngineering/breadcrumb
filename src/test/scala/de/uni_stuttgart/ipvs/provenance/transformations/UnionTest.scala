package de.uni_stuttgart.ipvs.provenance.transformations

import de.uni_stuttgart.ipvs.provenance.SharedSparkTestDataFrames
import de.uni_stuttgart.ipvs.provenance.nested_why_not.{Constants, WhyNotProvenance}
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.SchemaSubsetTree
import de.uni_stuttgart.ipvs.provenance.why_not_question.{Schema, SchemaBackTrace, Twig}
import org.scalatest.FunSuite

class UnionTest extends FunSuite with SharedSparkTestDataFrames {

  def basicWhyNotTuple(): Twig = {
    var twig = new Twig()
    val root = twig.createNode("root", 1, 1, "")
    val key = twig.createNode("key", 1, 1, "")
    twig = twig.createEdge(root, key, false)
    twig.validate().get
  }

  test("[Rewrite] Rewritten union retains previous provenance attributes") {
    val dfLeft = getDataFrame(pathToAggregationDoc0)
    val dfRight = getDataFrame(pathToUnionDoc0)
    val df = dfLeft.union(dfRight)
    val res = WhyNotProvenance.rewrite(df, basicWhyNotTuple())
    val leftRewrite = WhyNotProvenance.rewrite(dfLeft, basicWhyNotTuple())
    val rightRewrite = WhyNotProvenance.rewrite(dfRight, basicWhyNotTuple())
    val leftCnt = leftRewrite.columns.filter(name => Constants.columnNameContainsProvenanceConstant(name)).size
    val rightCnt = rightRewrite.columns.filter(name => Constants.columnNameContainsProvenanceConstant(name)).size
    val resCnt = res.columns.filter(name => Constants.columnNameContainsProvenanceConstant(name)).size
    assert(leftCnt + rightCnt + 1 == resCnt)
  }

  test("[Rewrite] Rewritten union retains all non-provenance attributes") {
    val dfLeft = getDataFrame(pathToAggregationDoc0)
    val dfRight = getDataFrame(pathToUnionDoc0)
    val df = dfLeft.union(dfRight)
    val res = WhyNotProvenance.rewrite(df, basicWhyNotTuple())
    res.show()
    res.explain(true)
    checkSchemaContainment(res, df)
  }


  def fullBasicWhyNotTuple(): Twig = {
    var twig = new Twig()
    val root = twig.createNode("root", 1, 1, "")
    val key = twig.createNode("key", 1, 1, "")
    val value = twig.createNode("value", 1, 1, "")
    twig = twig.createEdge(root, key, false)
    twig = twig.createEdge(root, value, false)
    twig.validate().get
  }


  test("[Unrestructure] Union with same attribute names") {
    val dfLeft = getDataFrame(pathToAggregationDoc0)
//    dfLeft = dfLeft.withColumnRenamed("value", "val")
    val dfRight = getDataFrame(pathToUnionDoc0)
    val res = dfLeft.union(dfRight)

    val schemaMatch = getSchemaMatch(res, fullBasicWhyNotTuple())
    val schemaSubset = SchemaSubsetTree(schemaMatch, new Schema(res))

    val plan = res.queryExecution.analyzed
    val rewrittenSchemaSubset = SchemaBackTrace(plan, schemaSubset).unrestructure()

    val child1RewrittenSchemaSubset = rewrittenSchemaSubset.head
    val child2RewrittenSchemaSubset = rewrittenSchemaSubset.last

    assert(schemaSubset.rootNode.name == child1RewrittenSchemaSubset.rootNode.name)
    assert(schemaSubset.rootNode.name == child2RewrittenSchemaSubset.rootNode.name)

    val key = child1RewrittenSchemaSubset.rootNode.children.find(node => node.name == "key").getOrElse(fail("key not where it is supposed to be"))
    val value = child1RewrittenSchemaSubset.rootNode.children.find(node => node.name == "value").getOrElse(fail("value not where it is supposed to be"))

    assert(key.name == "key")
    assert(value.name == "value")

    val key2 = child2RewrittenSchemaSubset.rootNode.children.find(node => node.name == "key").getOrElse(fail("key not where it is supposed to be"))
    val value2 = child2RewrittenSchemaSubset.rootNode.children.find(node => node.name == "value").getOrElse(fail("value not where it is supposed to be"))

    assert(key2.name == "key")
    assert(value2.name == "value")
  }


  test("[Unrestructure] Union with different attribute names") {
    val dfLeft = getDataFrame(pathToAggregationDoc0)
    val dfRight = getDataFrame(pathToJoinDoc0)
    val res = dfLeft.union(dfRight)

    val schemaMatch = getSchemaMatch(res, fullBasicWhyNotTuple())
    val schemaSubset = SchemaSubsetTree(schemaMatch, new Schema(res))

    val plan = res.queryExecution.analyzed
    val rewrittenSchemaSubset = SchemaBackTrace(plan, schemaSubset).unrestructure()

    val child1RewrittenSchemaSubset = rewrittenSchemaSubset.head
    val child2RewrittenSchemaSubset = rewrittenSchemaSubset.last

    assert(schemaSubset.rootNode.name == child1RewrittenSchemaSubset.rootNode.name)
    assert(schemaSubset.rootNode.name == child2RewrittenSchemaSubset.rootNode.name)

    val key = child1RewrittenSchemaSubset.rootNode.children.find(node => node.name == "key").getOrElse(fail("key not where it is supposed to be"))
    val value = child1RewrittenSchemaSubset.rootNode.children.find(node => node.name == "value").getOrElse(fail("value not where it is supposed to be"))

    assert(key.name == "key")
    assert(value.name == "value")

    val key2 = child2RewrittenSchemaSubset.rootNode.children.find(node => node.name == "key").getOrElse(fail("key not where it is supposed to be"))
    val value2 = child2RewrittenSchemaSubset.rootNode.children.find(node => node.name == "otherValue").getOrElse(fail("otherValue not where it is supposed to be"))

    assert(key2.name == "key")
    assert(value2.name == "otherValue")
  }


}
