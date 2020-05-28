package de.uni_stuttgart.ipvs.provenance.transformations

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import de.uni_stuttgart.ipvs.provenance.SharedSparkTestDataFrames
import de.uni_stuttgart.ipvs.provenance.nested_why_not.{Constants, WhyNotProvenance}
import de.uni_stuttgart.ipvs.provenance.why_not_question.Twig
import org.scalatest.FunSuite

class RelationTest extends FunSuite with SharedSparkTestDataFrames with DataFrameComparer {

  test("LocalRelation adds compatible column after rewrite") {
    val df = singleInputColumnDataFrame()
    val res = WhyNotProvenance.rewrite(df, myIntColWhyNotQuestion())
    res.schema.find(field => field.name.contains(Constants.COMPATIBLE_FIELD))
      .getOrElse(fail("Compatible Attribute is not added to input relation"))
  }



}
