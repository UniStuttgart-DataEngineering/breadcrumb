package de.uni_stuttgart.ipvs.provenance.transformations

import com.github.mrpowers.spark.fast.tests.{ColumnComparer, DataFrameComparer}
import de.uni_stuttgart.ipvs.provenance.SharedSparkTestDataFrames
import de.uni_stuttgart.ipvs.provenance.nested_why_not.{Constants, WhyNotProvenance}
import org.scalatest.FunSuite

class FilterTest extends FunSuite with SharedSparkTestDataFrames with DataFrameComparer with ColumnComparer{

  import spark.implicits._

  test("[Rewrite] Filter adds survived column") {
    val df = singleInputColumnDataFrame()
    val otherDf = df.filter($"MyIntCol" > 2)
    val res = WhyNotProvenance.rewrite(otherDf, whyNotTuple())
    assert(res.schema.filter(field => field.name.contains(Constants.SURVIVED_FIELD)).size == 1)
  }

  test("[Rewrite] Rewritten filter marks all items that do not survive the original filter with false") {
    val df = singleInputColumnDataFrame()
    val otherDf = df.filter($"MyIntCol" > 2)
    val res = WhyNotProvenance.rewrite(otherDf, whyNotTuple())
    val survivedColumn = res.schema.find(field => field.name.contains(Constants.SURVIVED_FIELD)).getOrElse(fail("SURVIVED FIELD NOT FOUND"))
    val filteredRes = res.filter(res.col(survivedColumn.name) === true).select($"MyIntCol")
    filteredRes.explain(true)
    assertSmallDataFrameEquality(otherDf, filteredRes)
  }

  test("[Rewrite] Filter copies the compatible column") {
    val df = singleInputColumnDataFrame()
    val otherDf = df.filter($"MyIntCol" > 2)
    val res = WhyNotProvenance.rewrite(otherDf, whyNotTuple())
    val compatibleFields = res.schema.filter(field => field.name.contains(Constants.COMPATIBLE_FIELD))
    assert(compatibleFields.size == 2)
    assertColumnEquality(res, compatibleFields(0).name, compatibleFields(1).name)
  }





}
