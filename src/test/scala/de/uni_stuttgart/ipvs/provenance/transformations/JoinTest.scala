package de.uni_stuttgart.ipvs.provenance.transformations

import com.github.mrpowers.spark.fast.tests.{ColumnComparer, DataFrameComparer}
import de.uni_stuttgart.ipvs.provenance.SharedSparkTestDataFrames
import de.uni_stuttgart.ipvs.provenance.nested_why_not.{Constants, WhyNotProvenance}
import de.uni_stuttgart.ipvs.provenance.why_not_question.Twig
import org.apache.spark.sql.catalyst.plans.LeftOuter
import org.scalatest.FunSuite

class JoinTest extends FunSuite with SharedSparkTestDataFrames with DataFrameComparer with ColumnComparer {

  import spark.implicits._

  def basicWhyNotTuple(): Twig = {
    var twig = new Twig()
    val root = twig.createNode("root", 1, 1, "")
    val key = twig.createNode("key", 1, 1, "")
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
    val res = WhyNotProvenance.rewrite(df, basicWhyNotTuple())
    assert(df.count() == res.count())

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
    val res = WhyNotProvenance.rewrite(df, basicWhyNotTuple())
    val lastSurvivedField = res.columns.filter(name => name.contains(Constants.SURVIVED_FIELD)).sorted.head
    val keyFourElements = res.filter($"key" === "4").select(res.col(lastSurvivedField)).map(row => row.getBoolean(0)).collect()
    assert(keyFourElements.size == 1)
    assert(keyFourElements.head == false)
  }

  test("[Rewrite] Left outer join survivorColumn") {
    val dfLeft = getDataFrame(pathToAggregationDoc0)
    val dfRight = getDataFrame(pathToJoinDoc0)
    val df = dfLeft.join(dfRight, Seq("key"), "leftouter")
    val res = WhyNotProvenance.rewrite(df, basicWhyNotTuple())
    val lastSurvivedField = res.columns.filter(name => name.contains(Constants.SURVIVED_FIELD)).sorted.head
    val keyFourElements = res.filter($"key" === "4").select(res.col(lastSurvivedField)).map(row => row.getBoolean(0)).collect()
    assert(keyFourElements.size == 1)
    assert(keyFourElements.head == false)
  }





}
