package de.uni_stuttgart.ipvs.provenance.why_not_question

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.plans.logical.{BinaryNode, Filter, LocalRelation, LogicalPlan, UnaryNode}
import org.apache.spark.sql.functions.monotonically_increasing_id
import org.scalatest.FunSuite

class RemoveFilter extends FunSuite {

  lazy val spark =
    SparkSession.builder
      .appName("SharedSparkTestInstance")
      .master("local[2]")
      .getOrCreate()

  import spark.implicits._

  def removeFilter(df: DataFrame): DataFrame = {
    val execState = df.queryExecution
    val planWithFilters = execState.analyzed
    val planWithoutFilters = removeFilterRecursive(planWithFilters)
    new DataFrame(
      execState.sparkSession,
      planWithoutFilters,
      RowEncoder(df.schema)
    )
  }

  def removeFilterRecursive(plan: LogicalPlan): LogicalPlan = {
    plan match {
      case filter: Filter => {
        removeFilterRecursive(filter.child)
      }
      case unaryNode: UnaryNode => {
        unaryNode.withNewChildren(Seq(removeFilterRecursive(unaryNode.child)))
      }
      case binaryNode: BinaryNode => {
        binaryNode.withNewChildren(Seq(removeFilterRecursive(binaryNode.left), removeFilterRecursive(binaryNode.right)))
      }
      case leaf: org.apache.spark.sql.catalyst.plans.logical.LeafNode => {
        leaf
      }
    }
  }

  def singleInputColumnDataFrame(): DataFrame = {
    Seq(1, 2, 3, 4, 5, 6).toDF("MyIntCol")
  }

  test("Query without filter remains the same"){
    var df = singleInputColumnDataFrame()
    df = df.withColumn("anotherCol", monotonically_increasing_id())
    val res = removeFilter(df)
    res.explain()
    res.show()
  }

  test("Query with filter does not remain the same"){
    var df = singleInputColumnDataFrame()
    df = df.filter($"MyIntCol" > 3)
    df.explain()
    df.show()
    val res = removeFilter(df)
    res.explain()
    res.show()
  }

  test("Query with filter does not remain the same 2"){
    var df = singleInputColumnDataFrame()
    df = df.filter($"MyIntCol" > 1)
    df = df.join(df, Seq("MyIntCol"))
    df = df.filter($"MyIntCol" < 5)
    df.explain()
    df.show()
    val res = removeFilter(df)
    res.explain()
    res.show()
  }






}
