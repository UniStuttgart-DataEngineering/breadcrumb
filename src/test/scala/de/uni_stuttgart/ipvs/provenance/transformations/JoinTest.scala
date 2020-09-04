package de.uni_stuttgart.ipvs.provenance.transformations

import com.github.mrpowers.spark.fast.tests.{ColumnComparer, DataFrameComparer}
import de.uni_stuttgart.ipvs.provenance.SharedSparkTestDataFrames
import de.uni_stuttgart.ipvs.provenance.nested_why_not.{Constants, WhyNotProvenance}
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.SchemaSubsetTree
import de.uni_stuttgart.ipvs.provenance.why_not_question.{Schema, SchemaBackTrace, Twig}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, LeafNode, Project}
import org.scalatest.FunSuite
import org.apache.spark.sql.functions.when

class JoinTest extends FunSuite with SharedSparkTestDataFrames with DataFrameComparer with ColumnComparer {

  import spark.implicits._

  def basicWhyNotTuple(): Twig = {
    var twig = new Twig()
    val root = twig.createNode("root", 1, 1, "")
    val key = twig.createNode("key", 1, 1, "")
    twig = twig.createEdge(root, key, false)
    twig.validate().get
  }

  def whyNotTupleWithOneCondition(): Twig = {
    var twig = new Twig()
    val root = twig.createNode("root", 1, 1, "")
    val key = twig.createNode("key", 1, 1, "1")
    twig = twig.createEdge(root, key, false)
    twig.validate().get
  }

  test("[Rewrite] Join adds survived column") {
    val dfLeft = getDataFrame(pathToAggregationDoc0)
    val dfRight = getDataFrame(pathToJoinDoc0)
    val df = dfLeft.join(dfRight, Seq("key"))
    val res = WhyNotProvenance.rewrite(df, basicWhyNotTuple())
    assert(res.schema.filter(field => field.name.contains(Constants.SURVIVED_FIELD)).size == 1)


  }

  test("[Rewrite] Rewritten join marks all items that do not survive the original join with false") {
    val dfLeft = getDataFrame(pathToAggregationDoc0)
    val dfRight = getDataFrame(pathToJoinDoc0)
    val df = dfLeft.join(dfRight, Seq("key"))
    val res = WhyNotProvenance.rewrite(df, whyNotTupleWithOneCondition())
    assert(df.count() + 2 == res.count())

    val survivedFields = res.columns.filter(name => name.contains(Constants.SURVIVED_FIELD))
    assert(survivedFields.size > 0, "Rewritten join does not add a survived field, see previous test")
    val orderedSurvivedFields = survivedFields.sorted
    val latestSurvivedField = orderedSurvivedFields(0)

    assert(df.count() == res.filter(res.col(latestSurvivedField) === true).count())
  }

  test("[Rewrite] Rewritten join retains previous provenance attributes") {
    val dfLeft = getDataFrame(pathToAggregationDoc0)
    val dfRight = getDataFrame(pathToJoinDoc0)
    val df = dfLeft.join(dfRight, Seq("key"))
    val res = WhyNotProvenance.rewrite(df, basicWhyNotTuple())
    val leftRewrite = WhyNotProvenance.rewrite(dfLeft, basicWhyNotTuple())
    val rightRewrite = WhyNotProvenance.rewrite(dfRight, basicWhyNotTuple())
    val leftCnt = leftRewrite.columns.filter(name => Constants.columnNameContainsProvenanceConstant(name)).size
    val rightCnt = rightRewrite.columns.filter(name => Constants.columnNameContainsProvenanceConstant(name)).size
    val resCnt = res.columns.filter(name => Constants.columnNameContainsProvenanceConstant(name)).size
    assert(leftCnt + rightCnt + 2 == resCnt)
  }

  test("[Rewrite] Rewritten join retains all non-provenance attributes") {
    val dfLeft = getDataFrame(pathToAggregationDoc0)
    val dfRight = getDataFrame(pathToJoinDoc0)
    val df = dfLeft.join(dfRight, Seq("key"))
    val res = WhyNotProvenance.rewrite(df, basicWhyNotTuple())
    checkSchemaContainment(res, df)
  }

  test("[Rewrite] Inner join becomes outer join and marks according items as not survived (false, not null)") {
    val dfLeft = getDataFrame(pathToAggregationDoc0)
    val dfRight = getDataFrame(pathToJoinDoc0)
    val df = dfLeft.join(dfRight, Seq("key"))
    val res = WhyNotProvenance.rewrite(df, whyNotTupleWithOneCondition())
    val lastSurvivedField = res.columns.filter(name => name.contains(Constants.SURVIVED_FIELD)).sorted.head
    val keyFourElements = res.filter($"key" === "4").select(res.col(lastSurvivedField)).map(row => row.getBoolean(0)).collect()
    assert(keyFourElements.size == 1)
    assert(keyFourElements.head == false)
  }

  test("[Rewrite] Left outer join survivorColumn") {
    val dfLeft = getDataFrame(pathToAggregationDoc0)
    val dfRight = getDataFrame(pathToJoinDoc0)
    val df = dfLeft.join(dfRight, Seq("key"), "leftouter")
    val res = WhyNotProvenance.rewrite(df, whyNotTupleWithOneCondition())
    val lastSurvivedField = res.columns.filter(name => name.contains(Constants.SURVIVED_FIELD)).sorted.head
    val keyFourElements = res.filter($"key" === "4").select(res.col(lastSurvivedField)).map(row => row.getBoolean(0)).collect()
    assert(keyFourElements.size == 1)
    assert(keyFourElements.head == false)
  }

  def whyNotTupleWithCondition(): Twig = {
    var twig = new Twig()
    val root = twig.createNode("root", 1, 1, "")
    val key = twig.createNode("key", 1, 1, "4")
    val key2 = twig.createNode("key2", 1, 1, "5")
    twig = twig.createEdge(root, key, false)
    twig = twig.createEdge(root, key2, false)
    twig.validate().get
  }

  test("[Rewrite] Ensure that compatible combinations survive after join") {
    val dfLeft = getDataFrame(pathToAggregationDoc0)
    val dfRight = getDataFrame(pathToJoinDoc0).withColumnRenamed("key", "key2")
    val df = dfLeft.join(dfRight, $"key" === $"key2")
    val res = WhyNotProvenance.rewrite(df, whyNotTupleWithCondition())
//    res.show()
    val lastSurvivedField = res.columns.filter(name => name.contains(Constants.SURVIVED_FIELD)).sorted.head
    val keyFourElements = res.filter($"key" === "4").select(res.col(lastSurvivedField)).map(row => row.getBoolean(0)).collect()
//    res.filter($"key" === 4).show()
    assert(keyFourElements.size == 1)
  }

  test("[Exploration] Make all compatibles survive") {
    val dfLeft = getDataFrame(pathToAggregationDoc0).withColumn("compatible1", when($"key" === 4, true).otherwise(false) )
    dfLeft.show()
    val dfRight = getDataFrame(pathToJoinDoc0).withColumnRenamed("key", "key2").withColumn("compatible2", when($"key2" === 5, true).otherwise(false))
    dfRight.show()
    val df = dfLeft.join(dfRight, $"key" === $"key2" || ($"compatible1" === true && $"compatible2" === true), "fullouter")
    df.show(100)
    df.explain(true)
  }

  def whyNotTupleWithConditionAlternatives(): Twig = {
    var twig = new Twig()
    val root = twig.createNode("root", 1, 1, "")
    val key = twig.createNode("key", 1, 1, "3")
    val jkey = twig.createNode("jkey", 1, 1, "3")
    val value = twig.createNode("value", 1, 1, "6")
    twig = twig.createEdge(root, key, false)
    twig = twig.createEdge(root, jkey, false)
    twig = twig.createEdge(root, value, false)
    twig.validate().get
  }

  test("[RewriteWithAlternatives] Join") {
    val dfLeft = getDataFrame(pathToSchemaAlternative)
    val dfRight = getDataFrame(pathToJoinDocWithAlternative).withColumnRenamed("key", "key2")
    val res = dfLeft.join(dfRight, $"key" === $"jkey")
    res.show()
    val provDf = WhyNotProvenance.rewriteWithAlternatives(res, whyNotTupleWithConditionAlternatives())
    provDf.show(50)
  }



  // Schema of left branch of join: JOIN - RELATION
  def getInputAndOutputWhyNotTupleLeft(outputDataFrame: DataFrame, outputWhyNotTuple: Twig): (SchemaSubsetTree, SchemaSubsetTree, SchemaSubsetTree) = {
    val schemaSubset = getSchemaSubsetTree(outputDataFrame, outputWhyNotTuple)
    val plan = outputDataFrame.queryExecution.analyzed
    val child = plan.children.head

    val rewrite1 = JoinRewrite(plan.asInstanceOf[Join], -1).undoLeftSchemaModifications(schemaSubset)
    val rewrite2 = RelationRewrite(child.asInstanceOf[LeafNode], 0).undoSchemaModifications(rewrite1)


    (rewrite1, rewrite2, schemaSubset)
  }


  // Schema of right branch of join: JOIN - PROJECT - RELATION
  def getInputAndOutputWhyNotTupleRight(outputDataFrame: DataFrame, outputWhyNotTuple: Twig): (SchemaSubsetTree, SchemaSubsetTree, SchemaSubsetTree, SchemaSubsetTree) = {
    val schemaSubset = getSchemaSubsetTree(outputDataFrame, outputWhyNotTuple)

    val plan = outputDataFrame.queryExecution.analyzed
    val child = plan.children.last
    val base = child.children.head

    val rewrite1 = JoinRewrite(plan.asInstanceOf[Join], -1).undoRightSchemaModifications(schemaSubset)
    val rewrite2 = ProjectRewrite(child.asInstanceOf[Project], 0).undoSchemaModifications(rewrite1)
    val rewrite3 = RelationRewrite(base.asInstanceOf[LeafNode], 0).undoSchemaModifications(rewrite2)

    (rewrite1, rewrite2, rewrite3, schemaSubset)
  }


  def fullWhyNotTupleBasicJoin(): Twig = {
    var twig = new Twig()
    val root = twig.createNode("root", 1, 1, "")
    val key = twig.createNode("key", 1, 1, "1")
    val value = twig.createNode("value", 1, 1, "")
    //    val key2 = twig.createNode("key", 1, 1, "")
    val value2 = twig.createNode("otherValue", 1, 1, "")
    twig = twig.createEdge(root, key, false)
    twig = twig.createEdge(root, value, false)
    //    twig = twig.createEdge(root, key, false)
    twig = twig.createEdge(root, value2, false)
    twig.validate().get
  }


//  test("[Unrestructure] 2-way join without renaming attributes") {
//    val dfLeft = getDataFrame(pathToAggregationDoc0)
//    val dfRight = getDataFrame(pathToJoinDoc0)
//    val res = dfLeft.join(dfRight, Seq("key"))
//
////    val (rewrittenSchemaSubset, schemaSubset) = getInputAndOutputWhyNotTuple(res, fullWhyNotTupleBasicJoin())
//
//    val schemaMatch = getSchemaMatch(res, fullWhyNotTupleBasicJoin())
//    val schemaSubset = SchemaSubsetTree(schemaMatch, new Schema(res))
//
//    val plan = res.queryExecution.analyzed
//    val rewrittenSchemaSubset = SchemaBackTrace(plan, schemaSubset).unrestructure()
//
//    val leftRewrittenSchemaSubset = rewrittenSchemaSubset.head
//    val rightRewrittenSchemaSubset = rewrittenSchemaSubset.last
//
//    assert(schemaSubset.rootNode.name == leftRewrittenSchemaSubset.rootNode.name)
//    assert(schemaSubset.rootNode.name == rightRewrittenSchemaSubset.rootNode.name)
//
//    val key = leftRewrittenSchemaSubset.rootNode.children.find(node => node.name == "key").getOrElse(fail("key not where it is supposed to be"))
//    val value = leftRewrittenSchemaSubset.rootNode.children.find(node => node.name == "value").getOrElse(fail("value not where it is supposed to be"))
//
//    assert(key.name == "key")
//    assert(value.name == "value")
//
//    val key2 = rightRewrittenSchemaSubset.rootNode.children.find(node => node.name == "key").getOrElse(fail("key not where it is supposed to be"))
//    val value2 = rightRewrittenSchemaSubset.rootNode.children.find(node => node.name == "otherValue").getOrElse(fail("otherValue not where it is supposed to be"))
//
//    assert(key2.name == "key")
//    assert(value2.name == "otherValue")
//  }


  def whyNotTupleBasicJoinSingleRef(): Twig = {
    var twig = new Twig()
    val root = twig.createNode("root", 1, 1, "")
    val key = twig.createNode("key", 1, 1, "1")
    twig = twig.createEdge(root, key, false)
    twig.validate().get
  }


//  test("[Unrestructure] 2-way join without renaming attributes (reference one attribute)") {
//    val dfLeft = getDataFrame(pathToAggregationDoc0)
//    val dfRight = getDataFrame(pathToJoinDoc0)
//    val res = dfLeft.join(dfRight, Seq("key"))
//
//    val schemaMatch = getSchemaMatch(res, whyNotTupleBasicJoinSingleRef())
//    val schemaSubset = SchemaSubsetTree(schemaMatch, new Schema(res))
//
//    val plan = res.queryExecution.analyzed
//    val rewrittenSchemaSubset = SchemaBackTrace(plan, schemaSubset).unrestructure()
//
//    val leftRewrittenSchemaSubset = rewrittenSchemaSubset.head
//    val rightRewrittenSchemaSubset = rewrittenSchemaSubset.last
//
//    assert(schemaSubset.rootNode.name == leftRewrittenSchemaSubset.rootNode.name)
//    assert(schemaSubset.rootNode.name == rightRewrittenSchemaSubset.rootNode.name)
//
//    assert(leftRewrittenSchemaSubset.rootNode.children.size == 1)
//    assert(rightRewrittenSchemaSubset.rootNode.children.size == 1)
//
//    val key = leftRewrittenSchemaSubset.rootNode.children.find(node => node.name == "key").getOrElse(fail("key not where it is supposed to be"))
//    assert(key.name == "key")
//
//    val key2 = rightRewrittenSchemaSubset.rootNode.children.find(node => node.name == "key").getOrElse(fail("key not where it is supposed to be"))
//    assert(key2.name == "key")
//  }


  def whyNotTupleBasicJoinSingleRef2(): Twig = {
    var twig = new Twig()
    val root = twig.createNode("root", 1, 1, "")
    val value = twig.createNode("value", 1, 1, "")
    twig = twig.createEdge(root, value, false)
    twig.validate().get
  }


//  test("[Unrestructure] 2-way join without renaming attributes (reference one attribute) 2") {
//    val dfLeft = getDataFrame(pathToAggregationDoc0)
//    val dfRight = getDataFrame(pathToJoinDoc0)
//    val res = dfLeft.join(dfRight, Seq("key"))
//
//    val schemaMatch = getSchemaMatch(res, whyNotTupleBasicJoinSingleRef2())
//    val schemaSubset = SchemaSubsetTree(schemaMatch, new Schema(res))
//
//    val plan = res.queryExecution.analyzed
//    val rewrittenSchemaSubset = SchemaBackTrace(plan, schemaSubset).unrestructure()
//    val leftRewrittenSchemaSubset = rewrittenSchemaSubset.head
//
//    assert(schemaSubset.rootNode.name == leftRewrittenSchemaSubset.rootNode.name)
//    assert(rewrittenSchemaSubset.size == 1)
//    assert(leftRewrittenSchemaSubset.rootNode.children.size == 1)
//
//    val value = leftRewrittenSchemaSubset.rootNode.children.find(node => node.name == "value").getOrElse(fail("value not where it is supposed to be"))
//    assert(value.name == "value")
//  }


  def fullWhyNotTupleBasicJoin2(): Twig = {
    var twig = new Twig()
    val root = twig.createNode("root", 1, 1, "")
    val key = twig.createNode("key", 1, 1, "")
    val value = twig.createNode("value", 1, 1, "")
    val key2 = twig.createNode("key2", 1, 1, "1")
    val value2 = twig.createNode("otherValue", 1, 1, "")
    twig = twig.createEdge(root, key, false)
    twig = twig.createEdge(root, value, false)
    twig = twig.createEdge(root, key2, false)
    twig = twig.createEdge(root, value2, false)
    twig.validate().get
  }


  test("[Unrestructure] Join with renaming the join attribute") {
    val dfLeft = getDataFrame(pathToAggregationDoc0)
    val dfRight = getDataFrame(pathToJoinDoc0).withColumnRenamed("key", "key2")
    val res = dfLeft.join(dfRight, $"key" === $"key2")

//    val schemaMatch = getSchemaMatch(res, fullWhyNotTupleBasicJoin2())
//    val schemaSubset = SchemaSubsetTree(schemaMatch, new Schema(res))
//
//    val plan = res.queryExecution.analyzed
//    val rewrittenSchemaSubset = SchemaBackTrace(plan, schemaSubset).unrestructure()
//
//    val leftRewrittenSchemaSubset = rewrittenSchemaSubset.head
//    val rightRewrittenSchemaSubset = rewrittenSchemaSubset.last

    // Evaluate left branch
    val (rewrittenSchemaSubsetLeft, rewrittenSchemaSubsetLeftRelation, schemaSubsetLeft) =
      getInputAndOutputWhyNotTupleLeft(res, fullWhyNotTupleBasicJoin2())

    assert(schemaSubsetLeft.rootNode.name == rewrittenSchemaSubsetLeft.rootNode.name)
    var key = rewrittenSchemaSubsetLeft.rootNode.children.find(node => node.name == "key").getOrElse(fail("key (left) not where it is supposed to be"))
    var value = rewrittenSchemaSubsetLeft.rootNode.children.find(node => node.name == "value").getOrElse(fail("value not where it is supposed to be"))

    assert(key.name == "key")
    assert(value.name == "value")

    key = rewrittenSchemaSubsetLeftRelation.rootNode.children.find(node => node.name == "key").getOrElse(fail("key not where it is supposed to be"))
    value = rewrittenSchemaSubsetLeftRelation.rootNode.children.find(node => node.name == "value").getOrElse(fail("value not where it is supposed to be"))

    assert(key.name == "key")
    assert(value.name == "value")

    // Evaluate right branch
    val (rewrittenSchemaSubsetRight, rewrittenSchemaSubsetRightProject, rewrittenSchemaSubsetRightRelation, schemaSubsetRight) =
      getInputAndOutputWhyNotTupleRight(res, fullWhyNotTupleBasicJoin2())

    assert(schemaSubsetRight.rootNode.name == rewrittenSchemaSubsetRight.rootNode.name)
    var key2 = rewrittenSchemaSubsetRight.rootNode.children.find(node => node.name == "key2").getOrElse(fail("key2 (right) not where it is supposed to be"))
    var value2 = rewrittenSchemaSubsetRight.rootNode.children.find(node => node.name == "otherValue").getOrElse(fail("otherValue not where it is supposed to be"))

    assert(key2.name == "key2")
    assert(value2.name == "otherValue")

    key2 = rewrittenSchemaSubsetRightProject.rootNode.children.find(node => node.name == "key").getOrElse(fail("key (right) not where it is supposed to be"))
    value2 = rewrittenSchemaSubsetRightProject.rootNode.children.find(node => node.name == "otherValue").getOrElse(fail("otherValue not where it is supposed to be"))

    assert(key2.name == "key")
    assert(value2.name == "otherValue")

    key2 = rewrittenSchemaSubsetRightRelation.rootNode.children.find(node => node.name == "key").getOrElse(fail("key (right) not where it is supposed to be"))
    value2 = rewrittenSchemaSubsetRightRelation.rootNode.children.find(node => node.name == "otherValue").getOrElse(fail("otherValue not where it is supposed to be"))

    assert(key2.name == "key")
    assert(value2.name == "otherValue")
  }


  test("[Unrestructure] Join with renaming attributes and reference one join attribute") {
    val dfLeft = getDataFrame(pathToAggregationDoc0)
    val dfRight = getDataFrame(pathToJoinDoc0).withColumnRenamed("key", "key2")
    val res = dfLeft.join(dfRight, $"key" === $"key2")

    // Evaluate left branch
    val (rewrittenSchemaSubsetLeft, rewrittenSchemaSubsetLeftRelation, schemaSubsetLeft) =
      getInputAndOutputWhyNotTupleLeft(res, whyNotTupleWithCondition())

    assert(schemaSubsetLeft.rootNode.name == rewrittenSchemaSubsetLeft.rootNode.name)

    assert(rewrittenSchemaSubsetLeft.rootNode.children.size == 1)
    var key = rewrittenSchemaSubsetLeft.rootNode.children.find(node => node.name == "key").getOrElse(fail("key (left) not where it is supposed to be"))
    assert(key.name == "key")

    assert(rewrittenSchemaSubsetLeftRelation.rootNode.children.size == 1)
    key = rewrittenSchemaSubsetLeftRelation.rootNode.children.find(node => node.name == "key").getOrElse(fail("key not where it is supposed to be"))
    assert(key.name == "key")


    // Evaluate right branch
    val (rewrittenSchemaSubsetRight, rewrittenSchemaSubsetRightProject, rewrittenSchemaSubsetRightRelation, schemaSubsetRight) =
      getInputAndOutputWhyNotTupleRight(res, whyNotTupleWithCondition())

    assert(schemaSubsetRight.rootNode.name == rewrittenSchemaSubsetRight.rootNode.name)

    assert(rewrittenSchemaSubsetRight.rootNode.children.size == 1)
    var key2 = rewrittenSchemaSubsetRight.rootNode.children.find(node => node.name == "key2").getOrElse(fail("key2 (right) not where it is supposed to be"))
    assert(key2.name == "key2")

    assert(rewrittenSchemaSubsetRightProject.rootNode.children.size == 2)
    key2 = rewrittenSchemaSubsetRightProject.rootNode.children.find(node => node.name == "key").getOrElse(fail("key (right) not where it is supposed to be"))
    rewrittenSchemaSubsetRightProject.rootNode.children.find(node => node.name == "otherValue").getOrElse(fail("otherValue (right) not where it is supposed to be"))


    assert(rewrittenSchemaSubsetRightRelation.rootNode.children.size == 2)
    key2 = rewrittenSchemaSubsetRightRelation.rootNode.children.find(node => node.name == "key").getOrElse(fail("key (right) not where it is supposed to be"))
    rewrittenSchemaSubsetRightProject.rootNode.children.find(node => node.name == "otherValue").getOrElse(fail("otherValue (right) not where it is supposed to be"))



    //
//    val schemaMatch = getSchemaMatch(res, whyNotTupleBasicJoinSingleRef())
//    val schemaSubset = SchemaSubsetTree(schemaMatch, new Schema(res))
//
//    val plan = res.queryExecution.analyzed
//    val rewrittenSchemaSubset = SchemaBackTrace(plan, schemaSubset).unrestructure()
//
//    val leftRewrittenSchemaSubset = rewrittenSchemaSubset.head
//    val rightRewrittenSchemaSubset = rewrittenSchemaSubset.last
//
//    assert(schemaSubset.rootNode.name == leftRewrittenSchemaSubset.rootNode.name)
//    assert(schemaSubset.rootNode.name == rightRewrittenSchemaSubset.rootNode.name)
//
//    assert(leftRewrittenSchemaSubset.rootNode.children.size == 1)
//    assert(rightRewrittenSchemaSubset.rootNode.children.size == 1)
//
//    val key = leftRewrittenSchemaSubset.rootNode.children.find(node => node.name == "key").getOrElse(fail("key not where it is supposed to be"))
//    assert(key.name == "key")
//
//    val key2 = rightRewrittenSchemaSubset.rootNode.children.find(node => node.name == "key").getOrElse(fail("key not where it is supposed to be"))
//    assert(key2.name == "key")
  }


  test("[Unrestructure] Join with renaming attributes and reference one attribute from left") {
    val dfLeft = getDataFrame(pathToAggregationDoc0)
    val dfRight = getDataFrame(pathToJoinDoc0).withColumnRenamed("key", "key2")
    val res = dfLeft.join(dfRight, $"key" === $"key2")

    // Evaluate left branch
    val (rewrittenSchemaSubsetLeft, rewrittenSchemaSubsetLeftRelation, schemaSubsetLeft) =
      getInputAndOutputWhyNotTupleLeft(res, whyNotTupleBasicJoinSingleRef2())

    assert(schemaSubsetLeft.rootNode.name == rewrittenSchemaSubsetLeft.rootNode.name)

    assert(rewrittenSchemaSubsetLeft.rootNode.children.size == 2)
    rewrittenSchemaSubsetLeft.rootNode.children.find(node => node.name == "value").getOrElse(fail("value not where it is supposed to be"))
    rewrittenSchemaSubsetLeft.rootNode.children.find(node => node.name == "key").getOrElse(fail("key not where it is supposed to be"))

    assert(rewrittenSchemaSubsetLeftRelation.rootNode.children.size == 2)
    rewrittenSchemaSubsetLeftRelation.rootNode.children.find(node => node.name == "value").getOrElse(fail("value not where it is supposed to be"))
    rewrittenSchemaSubsetLeftRelation.rootNode.children.find(node => node.name == "key").getOrElse(fail("key not where it is supposed to be"))


    // Evaluate right branch
    val (rewrittenSchemaSubsetRight, rewrittenSchemaSubsetRightProject, rewrittenSchemaSubsetRightRelation, schemaSubsetRight) =
      getInputAndOutputWhyNotTupleRight(res, whyNotTupleBasicJoinSingleRef2())

    assert(schemaSubsetRight.rootNode.name == rewrittenSchemaSubsetRight.rootNode.name)
    assert(rewrittenSchemaSubsetRight.rootNode.children.size == 1)
    rewrittenSchemaSubsetRight.rootNode.children.find(node => node.name == "key2").getOrElse(fail("key2 (right) not where it is supposed to be"))
    assert(rewrittenSchemaSubsetRightProject.rootNode.children.size == 2)
    rewrittenSchemaSubsetRightProject.rootNode.children.find(node => node.name == "key").getOrElse(fail("key (right) not where it is supposed to be"))
    rewrittenSchemaSubsetRightProject.rootNode.children.find(node => node.name == "otherValue").getOrElse(fail("otherValue (right) not where it is supposed to be"))
    assert(rewrittenSchemaSubsetRightRelation.rootNode.children.size == 2)
    rewrittenSchemaSubsetRightRelation.rootNode.children.find(node => node.name == "key").getOrElse(fail("key (right) not where it is supposed to be"))
    rewrittenSchemaSubsetRightRelation.rootNode.children.find(node => node.name == "otherValue").getOrElse(fail("otherValue (right) not where it is supposed to be"))



    //
//    val schemaMatch = getSchemaMatch(res, whyNotTupleBasicJoinSingleRef2())
//    val schemaSubset = SchemaSubsetTree(schemaMatch, new Schema(res))
//
//    val plan = res.queryExecution.analyzed
//    val rewrittenSchemaSubset = SchemaBackTrace(plan, schemaSubset).unrestructure()
//    val leftRewrittenSchemaSubset = rewrittenSchemaSubset.head
//
//    assert(schemaSubset.rootNode.name == leftRewrittenSchemaSubset.rootNode.name)
//    assert(rewrittenSchemaSubset.size == 1)
//    assert(leftRewrittenSchemaSubset.rootNode.children.size == 1)
//
//    val value = leftRewrittenSchemaSubset.rootNode.children.find(node => node.name == "value").getOrElse(fail("value not where it is supposed to be"))
//    assert(value.name == "value")
  }


//  test("[Unrestructure] JoinWith") {
//    val dfLeft = getDataFrame(pathToAggregationDoc0)
//    val dfRight = getDataFrame(pathToJoinDoc0)
//    val df = dfLeft.joinWith(dfRight, dfLeft("key") === dfRight("key"))
//
//    dfLeft.show(false)
//    println(dfLeft.queryExecution.analyzed)
//
//    dfRight.show(false)
//    println(dfRight.queryExecution.analyzed)
//
//    df.show(false)
//    println(df.queryExecution.analyzed)
//  }
//
//
//  test("[Unrestructure] CrossJoin") {
//    val dfLeft = getDataFrame(pathToAggregationDoc0)
//    val dfRight = getDataFrame(pathToJoinDoc0)
//    val df = dfLeft.crossJoin(dfRight)
////    val df = dfLeft.join(dfRight, Seq("key"), "fullouter")
//
//    dfLeft.show(false)
//    println(dfLeft.queryExecution.analyzed)
//
//    dfRight.show(false)
//    println(dfRight.queryExecution.analyzed)
//
//    df.show(false)
//    println(df.queryExecution.analyzed)
//  }


}
