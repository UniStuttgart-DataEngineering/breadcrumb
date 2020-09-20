package de.uni_stuttgart.ipvs.provenance.evaluation

import de.uni_stuttgart.ipvs.provenance.SharedSparkTestDataFrames
import de.uni_stuttgart.ipvs.provenance.evaluation.dblp.DBLPScenario1
import de.uni_stuttgart.ipvs.provenance.evaluation.twitter.{TwitterScenario1, TwitterScenario2, TwitterScenario3, TwitterScenario4, TwitterScenario5, TwitterScenario6}
import de.uni_stuttgart.ipvs.provenance.nested_why_not.ProvenanceContext
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions.{count, explode, to_date, typedLit, from_unixtime, dayofmonth}
import org.apache.spark.sql.types.{DateType, LongType}
import org.scalatest.FunSuite

class TwitterScenarios extends FunSuite with SharedSparkTestDataFrames {

  import spark.implicits._
  val pathToData = "src/main/external_resources/TwitterData/"
  val testConfiguration1 = TestConfiguration.local(pathToData)

  def collectDataFrameLocal(df: DataFrame, scenarioName: String): Unit = {
    df.write.mode(SaveMode.Overwrite).parquet(pathToData + scenarioName)
    df.explain()
  }

//  SCENARIO 1
  test("[Reference] Scenario 1"){
    val scenario = new TwitterScenario1(spark, testConfiguration1)
    scenario.referenceScenario.show(10, false)
  }

  test("[RewriteWithoutSA] Scenario 1"){
    val scenario = new TwitterScenario1(spark, testConfiguration1)
    scenario.extendedScenarioWithoutSA.show(10)
  }

  test("[RewriteWithSA] Scenario 1"){
    val scenario = new TwitterScenario1(spark, testConfiguration1)
//    scenario.extendedScenarioWithSA.show(10)
    ProvenanceContext.setTestScenario(scenario)
//    scenario.extendedScenarioWithSA
    val toBeDebugged = scenario.extendedScenarioWithSA.filter($"id_str".contains("1027612080084414464"))
    toBeDebugged.explain()
    toBeDebugged.show() //.withColumn("prov", explode($"__PROVENANCE_COLLECTION_0001")).show(50)
    ProvenanceContext.setTestScenario(null)
  }

  test("[MSR] Scenario 1"){
    val scenario = new TwitterScenario1(spark, testConfiguration1)
    scenario.extendedScenario.show(10)
  }

  test("[RewriteWithSAMSR] Scenario 1") {
    val scenario = new TwitterScenario1(spark, testConfiguration1)
    ProvenanceContext.setTestScenario(scenario)
    val res = scenario.extendedScenarioWithSAandMSR()
    res.show(false)
    res.explain()
    ProvenanceContext.setTestScenario(null)
  }

  test("[RewriteWithPreparedSAMSR] Scenario 1") {
    val scenario = new TwitterScenario1(spark, testConfiguration1)
    ProvenanceContext.setTestScenario(scenario)
    var res = scenario.prepareScenarioForMSRComputation()
    collectDataFrameLocal(res, scenario.getName + "intermediate")
    res = scenario.extendedScenarioWithPreparedSAandMSR()
    res.show(10)
    ProvenanceContext.setTestScenario(null)
  }




//  SCENARIO 2
  test("[Reference] Scenario 2"){
    val scenario = new TwitterScenario2(spark, testConfiguration1)
    scenario.referenceScenario.show(10)
  }

  test("[RewriteWithoutSA] Scenario 2"){
    val scenario = new TwitterScenario2(spark, testConfiguration1)
    scenario.extendedScenarioWithoutSA.show(10)
  }

  test("[RewriteWithSA] Scenario 2") {
    val scenario = new TwitterScenario2(spark, testConfiguration1)
//    scenario.extendedScenarioWithSA.show(10)
    ProvenanceContext.setTestScenario(scenario)
//    scenario.extendedScenarioWithSA
    var toBeDebugged = scenario.extendedScenarioWithSA
//    toBeDebugged = toBeDebugged.withColumn("lname", explode($"listOfNames")).filter($"lname".contains("Cindy"))
    toBeDebugged.explain()
    toBeDebugged.show() //.withColumn("prov", explode($"__PROVENANCE_COLLECTION_0001")).show(50)
    ProvenanceContext.setTestScenario(null)
  }

  test("[MSR] Scenario 2"){
    val scenario = new TwitterScenario2(spark, testConfiguration1)
    scenario.extendedScenario.show(10)
  }

  test("[RewriteWithSAMSR] Scenario 2") {
    val scenario = new TwitterScenario2(spark, testConfiguration1)
    ProvenanceContext.setTestScenario(scenario)
    val res = scenario.extendedScenarioWithSAandMSR()
    res.show(false)
    res.explain()
    ProvenanceContext.setTestScenario(null)
  }

  test("[RewriteWithPreparedSAMSR] Scenario 2") {
    val scenario = new TwitterScenario2(spark, testConfiguration1)
    ProvenanceContext.setTestScenario(scenario)
    var res = scenario.prepareScenarioForMSRComputation()
    collectDataFrameLocal(res, scenario.getName + "intermediate")
    res = scenario.extendedScenarioWithPreparedSAandMSR()
    res.show(10)
    ProvenanceContext.setTestScenario(null)
  }



// SCENARIO 3
//  test("[Reference] Scenario 3"){
//    val scenario = new TwitterScenario3(spark, testConfiguration1)
//    scenario.referenceScenario //.filter($"screen_name".contains("YouTube")).show(50, false)
//  }
//
//  test("[RewriteWithoutSA] Scenario 3"){
//    val scenario = new TwitterScenario3(spark, testConfiguration1)
//    scenario.extendedScenarioWithoutSA //.filter($"screen_name".contains("YouTube")).show(10)
//  }
//
//  test("[RewriteWithSA] Scenario 3") {
//    val scenario = new TwitterScenario3(spark, testConfiguration1)
//    //    scenario.extendedScenarioWithSA.show(10)
//    ProvenanceContext.setTestScenario(scenario)
//    //    scenario.extendedScenarioWithSA
//    val toBeDebugged = scenario.extendedScenarioWithSA.filter($"screen_name".contains("YouTube"))
//    toBeDebugged.explain()
//    toBeDebugged.show() //.withColumn("prov", explode($"__PROVENANCE_COLLECTION_0001")).show(50)
//    ProvenanceContext.setTestScenario(null)
//  }
//
//  test("[MSR] Scenario 3"){
//    val scenario = new TwitterScenario3(spark, testConfiguration1)
//    scenario.extendedScenario.show(10)
//    //    scenario.extendedScenario().explain(true)
//  }


  test("[Reference] Scenario 3"){
    val scenario = new TwitterScenario6(spark, testConfiguration1)
    scenario.referenceScenario.filter($"name".contains("Coca Cola")).show(50, false)
  }

  test("[RewriteWithoutSA] Scenario 3"){
    val scenario = new TwitterScenario6(spark, testConfiguration1)
    scenario.extendedScenarioWithoutSA.filter($"name".contains("Vanessa Tuqueque")).show(50, false)
  }

  test("[RewriteWithSA] Scenario 3") {
    val scenario = new TwitterScenario6(spark, testConfiguration1)
    ProvenanceContext.setTestScenario(scenario)
//    scenario.extendedScenarioWithSA.explain(true)
    scenario.extendedScenarioWithSA.filter($"name".contains("Coca cola")).show(20)
    ProvenanceContext.setTestScenario(null)
  }

  test("[MSR] Scenario 3"){
    val scenario = new TwitterScenario6(spark, testConfiguration1)
    scenario.extendedScenario.show(10)
    //    scenario.extendedScenario().explain(true)
  }

  test("[RewriteWithSAMSR] Scenario 3") {
    val scenario = new TwitterScenario6(spark, testConfiguration1)
    ProvenanceContext.setTestScenario(scenario)
    val res = scenario.extendedScenarioWithSAandMSR()
    res.show(false)
    res.explain()
    ProvenanceContext.setTestScenario(null)
  }

  test("[RewriteWithPreparedSAMSR] Scenario 3") {
    val scenario = new TwitterScenario6(spark, testConfiguration1)
    ProvenanceContext.setTestScenario(scenario)
    var res = scenario.prepareScenarioForMSRComputation()
    collectDataFrameLocal(res, scenario.getName + "intermediate")
    res = scenario.extendedScenarioWithPreparedSAandMSR()
    res.show(10)
    ProvenanceContext.setTestScenario(null)
  }




  // SCENARIO 4
  test("[Reference] Scenario 4"){
    val scenario = new TwitterScenario4(spark, testConfiguration1)
    var res = scenario.referenceScenario
//    res = res.filter($"hashtagText".contains("Warcraft"))
    res.show(20, false)
  }

  test("[RewriteWithoutSA] Scenario 4"){
    val scenario = new TwitterScenario4(spark, testConfiguration1)
    scenario.extendedScenarioWithoutSA.show(10)
  }

  test("[RewriteWithSA] Scenario 4") {
    val scenario = new TwitterScenario4(spark, testConfiguration1)
    //    scenario.extendedScenarioWithSA.show(10)
    ProvenanceContext.setTestScenario(scenario)
//    scenario.extendedScenarioWithSA
    var toBeDebugged = scenario.extendedScenarioWithSA.filter($"hashtagText".contains("Arsenal"))
    toBeDebugged.explain()
    toBeDebugged.show() //.withColumn("prov", explode($"__PROVENANCE_COLLECTION_0001")).show(50)
    ProvenanceContext.setTestScenario(null)
  }

  test("[MSR] Scenario 4"){
    val scenario = new TwitterScenario4(spark, testConfiguration1)
    scenario.extendedScenario.show(10)
  }

  test("[RewriteWithSAMSR] Scenario 4") {
    val scenario = new TwitterScenario4(spark, testConfiguration1)
    ProvenanceContext.setTestScenario(scenario)
    val res = scenario.extendedScenarioWithSAandMSR()
    res.show(false)
    res.explain()
    ProvenanceContext.setTestScenario(null)
  }

  test("[RewriteWithPreparedSAMSR] Scenario 4") {
    val scenario = new TwitterScenario4(spark, testConfiguration1)
    ProvenanceContext.setTestScenario(scenario)
    var res = scenario.prepareScenarioForMSRComputation()
    collectDataFrameLocal(res, scenario.getName + "intermediate")
    res = scenario.extendedScenarioWithPreparedSAandMSR()
    res.show(10)
    ProvenanceContext.setTestScenario(null)
  }




  // SCENARIO 5
  test("[Reference] Scenario 5"){
    val scenario = new TwitterScenario5(spark, testConfiguration1)
    scenario.referenceScenario.show(50, false)
  }

  test("[RewriteWithoutSA] Scenario 5"){
    val scenario = new TwitterScenario5(spark, testConfiguration1)
    scenario.extendedScenarioWithoutSA.show(10)
  }

  test("[RewriteWithSA] Scenario 5") {
    val scenario = new TwitterScenario5(spark, testConfiguration1)
    //    scenario.extendedScenarioWithSA.show(10)
    ProvenanceContext.setTestScenario(scenario)
//    scenario.extendedScenarioWithSA
    val toBeDebugged = scenario.extendedScenarioWithSA.filter($"pname".contains("San Diego"))
    toBeDebugged.explain()
    toBeDebugged.show(false) //.withColumn("prov", explode($"__PROVENANCE_COLLECTION_0001")).show(50)
    ProvenanceContext.setTestScenario(null)
  }

  test("[MSR] Scenario 5"){
    val scenario = new TwitterScenario5(spark, testConfiguration1)
    scenario.extendedScenario.show(10)
  }

  test("[RewriteWithSAMSR] Scenario 5") {
    val scenario = new TwitterScenario5(spark, testConfiguration1)
    ProvenanceContext.setTestScenario(scenario)
    val res = scenario.extendedScenarioWithSAandMSR()
    res.show(false)
//    res.explain()
    ProvenanceContext.setTestScenario(null)
  }

  test("[RewriteWithPreparedSAMSR] Scenario 5") {
    val scenario = new TwitterScenario5(spark, testConfiguration1)
    ProvenanceContext.setTestScenario(scenario)
    var res = scenario.prepareScenarioForMSRComputation()
    collectDataFrameLocal(res, scenario.getName + "intermediate")
    res = scenario.extendedScenarioWithPreparedSAandMSR()
    res.show(10)
    ProvenanceContext.setTestScenario(null)
  }


//  test("[Reference] Scenario 6"){
//    val scenario = new TwitterScenario6(spark, testConfiguration1)
//    scenario.referenceScenario.filter($"name".contains("Vanessa Tuqueque")).show(50, false)
//  }
//
//  test("[Reference] Scenario 6a"){
//    val scenario = new TwitterScenario6(spark, testConfiguration1)
//    scenario.referenceScenario.filter($"name".contains("American Express")).show(50, false)
//  }
//
//  test("[RewriteWithSA] Scenario 6") {
//    val scenario = new TwitterScenario6(spark, testConfiguration1)
//    ProvenanceContext.setTestScenario(scenario)
//    scenario.extendedScenarioWithSA.filter($"name".contains("Vanessa Tuqueque")).show(20)
//    ProvenanceContext.setTestScenario(null)
//  }

  def loadTweets(): DataFrame = {
    val completePath = pathToData + "*" + ".log"
    spark.read.json(completePath)
  }

  test("exploration") {
    val tweets = loadTweets()
    var res = tweets.select("created_at").groupBy("created_at").agg(count(typedLit(1)).alias("cnt"))
    res = tweets.select(to_date($"created_at", "EEE MMM d hh:mm:ss '+0000' yyyy").alias("date"), $"created_at", dayofmonth(to_date(from_unixtime($"timestamp_ms".cast(LongType) / 1000))).alias("day"))

    //res.orderBy($"cnt")
    res.show(20, false)
  }

  test("exploration2") {
    import java.text.SimpleDateFormat
    val value = "Thu Aug 09 17:00:00 +0000 2018"

    val pattern = "EEE MMM d hh:mm:ss '+0000' yyyy"
    val formatter = new SimpleDateFormat(pattern)
    val format = DateTimeUtils.newDateFormat("EEE MMM d hh:mm:ss '+0000' yyyy", DateTimeUtils.getTimeZone("UTC"))

    val output = formatter.parse(value)

    System.out.println(pattern + " " + output)
  }

}
