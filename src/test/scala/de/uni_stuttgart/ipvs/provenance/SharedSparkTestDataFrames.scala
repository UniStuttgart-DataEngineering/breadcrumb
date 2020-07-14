package de.uni_stuttgart.ipvs.provenance

import de.uni_stuttgart.ipvs.provenance.schema_alternatives.SchemaSubsetTree
import de.uni_stuttgart.ipvs.provenance.transformations.{FilterRewrite, JoinRewrite, ProjectRewrite, RelationRewrite, UnionRewrite}
import de.uni_stuttgart.ipvs.provenance.why_not_question.{Schema, SchemaMatch, SchemaMatcher, Twig}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, LeafNode, LogicalPlan, Project, Union}
import org.apache.spark.sql.execution.datasources.LogicalRelation

trait SharedSparkTestDataFrames extends SharedSparkTestInstance {

  import spark.implicits._

  protected val baseDir = "src/main/resources/"

  protected val pathToNestedData0 = baseDir + "nestedCollection_Simple.json"
  protected val pathToNestedData00 = baseDir + "nestedCollection_WithEmpty.json"
  protected val pathToNestedData1 = baseDir + "nestedData.json"
  protected val pathToNestedData2 = baseDir + "nestedData_moreComplex.json"
  protected val pathToNestedData3 = baseDir + "nestedData_withKeyDuplicates.json"
  protected val pathToNestedData4 = baseDir + "nestedData_moreComplex_renamed.json"
  protected val pathToNestedData5 = baseDir + "nestedData_renamedKeys.json"
  protected val pathToNestedData6 = baseDir + "nestedData_deepNested.json"
  protected var pathToNestedData7 = baseDir + "orders.json"
  protected var pathToNestedData8 = baseDir + "countMinMax.json"
  protected val pathToTweetsDummy = baseDir + "tweets_dummy.json"
  protected val pathToDoc1 = baseDir + "doc1.json"
  protected val pathToDoc2 = baseDir + "doc2.json"
  protected val pathToDoc3 = baseDir + "doc3.json"
  protected val pathToDoc4 = baseDir + "doc4.json"
  protected val pathToDemoData = baseDir + "demo.json"
  protected val pathToDemoData1 = baseDir + "demoOneStruct.json"
  protected val pathToAggregationDoc0 = baseDir + "docAggregation.json"
  protected val pathToJoinDoc0 = baseDir + "docJoin.json"
  protected val pathToUnionDoc0 = baseDir + "docUnion.json"
  protected val pathToExampleData = baseDir + "exampleData.json"

  def myIntColWhyNotQuestion(): Twig = {
    var twig = new Twig()
    val root = twig.createNode("root", 1, 1, "")
    val flat_key = twig.createNode("MyIntCol", 1, 1, "")
    twig = twig.createEdge(root, flat_key, false)
    twig.validate().get
  }

  def myIntColWhyNotQuestionWithCondition(): Twig = {
    var twig = new Twig()
    val root = twig.createNode("root", 1, 1, "")
    val flat_key = twig.createNode("MyIntCol", 1, 1, "3")
    twig = twig.createEdge(root, flat_key, false)
    twig.validate().get
  }

  def myIntColWhyNotQuestionWithConditionGT(): Twig = {
    var twig = new Twig()
    val root = twig.createNode("root", 1, 1, "")
    val flat_key = twig.createNode("MyIntCol", 1, 1, "gtgtgtgt4")
    twig = twig.createEdge(root, flat_key, false)
    twig.validate().get
  }

  def singleInputColumnDataFrame(): DataFrame = {
    Seq(1, 2, 3, 4, 5, 6).toDF("MyIntCol")
  }


  def getDataFrame(): DataFrame = {
    val initial: DataFrame = getDataFrame(pathToNestedData2)
    initial.filter(initial.col("flat_key") isNotNull)
  }

  def getDataFrame(path: String): DataFrame = {
    spark.read.json(path)
  }

  def getSchemaMatch(df: DataFrame, twig: Twig): SchemaMatch  = {
    val schema = new Schema(df)
    val schemaMatcher = SchemaMatcher(twig, schema)
    schemaMatcher.getCandidate().getOrElse(throw new MatchError("The why not question either does not match or matches multiple times on the given dataframe schema."))
  }

  def whyNotTuple(): Twig = {
    var twig = new Twig()
    val root = twig.createNode("root", 1, 1, "")
    val flat_key = twig.createNode("nested_list", 1, 1, "")
    twig = twig.createEdge(root, flat_key, false)
    twig.validate().get
  }

  def whyNotTupleWithCond(): Twig = {
    var twig = new Twig()
    val root = twig.createNode("root", 1, 1, "")
    val flat_key = twig.createNode("MyIntCol", 1, 1, "> 4")
    twig = twig.createEdge(root, flat_key, false)
    twig.validate().get
  }


  def getSchemaSubsetTree(outputDataFrame: DataFrame, outputWhyNotTuple: Twig): SchemaSubsetTree = {
    val schemaMatch = getSchemaMatch(outputDataFrame, outputWhyNotTuple)
    val schemaSubset = SchemaSubsetTree(schemaMatch, new Schema(outputDataFrame))

    schemaSubset
  }


  def getInputAndOutputWhyNotTupleFlex(plan: LogicalPlan, schemaSubset: SchemaSubsetTree, branch: String): SchemaSubsetTree = {
    var rewrite: SchemaSubsetTree = null

    plan match {
      case p: Project => rewrite = ProjectRewrite(p, -1).undoSchemaModifications(schemaSubset)
      case f: Filter => rewrite = FilterRewrite(f, -1).undoSchemaModifications(schemaSubset)
      case j: Join => {
        if (branch.equals("L")) {
          val lChild = j.left
          rewrite = JoinRewrite(j, -1).undoLeftSchemaModifications(schemaSubset)
          rewrite = RelationRewrite(lChild.asInstanceOf[LeafNode], 0).undoSchemaModifications(rewrite)
        }
        if (branch.equals("R")) {
          val rChild = j.right
          rewrite = JoinRewrite(j, -1).undoRightSchemaModifications(schemaSubset)
          rewrite = ProjectRewrite(rChild.asInstanceOf[Project], 0).undoSchemaModifications(rewrite)
          rewrite = RelationRewrite(rChild.children.head.asInstanceOf[LeafNode], 1).undoSchemaModifications(rewrite)
        }
      }
      case u: Union => {
        if (branch.equals("L")) {
          val lChild = u.children.head
          rewrite = UnionRewrite(u, -1).undoLeftSchemaModifications(schemaSubset)
          rewrite = RelationRewrite(lChild.asInstanceOf[LeafNode], 0).undoSchemaModifications(rewrite)
        }
        if (branch.equals("R")) {
          val rChild = u.children.last
          rewrite = UnionRewrite(u, -1).undoRightSchemaModifications(schemaSubset)
          rewrite = RelationRewrite(rChild.children.head.asInstanceOf[LeafNode], 1).undoSchemaModifications(rewrite)
        }
      }
      case l: LeafNode => rewrite = RelationRewrite(l, -1).undoSchemaModifications(schemaSubset)
    }

    rewrite
  }


}
