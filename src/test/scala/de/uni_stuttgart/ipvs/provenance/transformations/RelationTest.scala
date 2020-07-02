package de.uni_stuttgart.ipvs.provenance.transformations

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import de.uni_stuttgart.ipvs.provenance.SharedSparkTestDataFrames
import de.uni_stuttgart.ipvs.provenance.nested_why_not.{Constants, WhyNotProvenance}
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.SchemaSubsetTree
import de.uni_stuttgart.ipvs.provenance.why_not_question.{DataFetcher, Twig}
import org.scalatest.FunSuite

class RelationTest extends FunSuite with SharedSparkTestDataFrames with DataFrameComparer {

  import spark.implicits._

  test("[Rewrite] LocalRelation adds compatible column after rewrite") {
    val df = singleInputColumnDataFrame()
    val res = WhyNotProvenance.rewrite(df, myIntColWhyNotQuestion())
    res.schema.find(field => field.name.contains(Constants.COMPATIBLE_FIELD))
      .getOrElse(fail("Compatible Attribute is not added to input relation"))
  }

  test("[Rewrite] LocalRelation retains all items") {
    val df = singleInputColumnDataFrame()
    val res = WhyNotProvenance.rewrite(df, myIntColWhyNotQuestion())
    assert(df.count() == res.count())
  }

  test("[Rewrite] LocalRelation marks the 3 as compatible") {
    val df = singleInputColumnDataFrame()
    val res = WhyNotProvenance.rewrite(df, myIntColWhyNotQuestionWithCondition())
    val compatibleCol = res.schema.find(field => field.name.contains(Constants.COMPATIBLE_FIELD))
      .getOrElse(fail("Compatible Attribute is not added to input relation"))
    val compatibleSeq = res.filter($"MyIntCol" === 3).select(res.col(compatibleCol.name)).map(row => row.getBoolean(0)).collect()
    assert(compatibleSeq.size == 1)
    assert(compatibleSeq(0) == true)
  }

  test("[Rewrite] LocalRelation marks all columns but the 3 as non-compatible") {
    val df = singleInputColumnDataFrame()
    val res = WhyNotProvenance.rewrite(df, myIntColWhyNotQuestionWithCondition())
    res.explain(true)
    res.show()
    val compatibleCol = res.schema.find(field => field.name.contains(Constants.COMPATIBLE_FIELD))
      .getOrElse(fail("Compatible Attribute is not added to input relation"))
    val nonCompatibleCnt = res.filter(res.col(compatibleCol.name) === false).count()
    assert(nonCompatibleCnt == df.count - 1)
  }

  test("[Rewrite] Relation marks all columns but the 3 as non-compatible") {
    val df = singleInputColumnDataFrame()
    val res = WhyNotProvenance.rewrite(df, myIntColWhyNotQuestionWithCondition())
    res.explain(true)
    res.show()
    val compatibleCol = res.schema.find(field => field.name.contains(Constants.COMPATIBLE_FIELD))
      .getOrElse(fail("Compatible Attribute is not added to input relation"))
    val nonCompatibleCnt = res.filter(res.col(compatibleCol.name) === false).count()
    assert(nonCompatibleCnt == df.count - 1)
  }









}
