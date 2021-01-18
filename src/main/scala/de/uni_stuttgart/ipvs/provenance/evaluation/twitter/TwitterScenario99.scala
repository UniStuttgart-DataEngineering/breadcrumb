package de.uni_stuttgart.ipvs.provenance.evaluation.twitter

import de.uni_stuttgart.ipvs.provenance.evaluation.TestConfiguration
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.{PrimarySchemaSubsetTree, SchemaNode, SchemaSubsetTree}
import de.uni_stuttgart.ipvs.provenance.why_not_question.Twig
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, explode}

class TwitterScenario99 (spark: SparkSession, testConfiguration: TestConfiguration) extends TwitterScenario (spark, testConfiguration){

  override def getName(): String = "T99"

  import spark.implicits._



  override def referenceScenario(): DataFrame = {
    return referenceScenario1()
      /*
      .select(
      $"q_text",
      $"r_text",
      $"q_retweet_count",
      $"r_retweet_count",
      $"q_quote_count",
      $"r_quote_count")
        .filter($"q_retweet_count" === 0 && $"r_quote_count" > 0) */
    var tw = loadTweets()
    //tw = tw.withColumn("retweet_info", explode($"retweeted_status"))
    //tw = tw.filter($"retweeted" === true)
    tw = tw.select(col("retweeted_status.*"))//, col("quoted_status.*"))
    tw = tw.filter($"retweeted" === true)
    //tw = tw.select($"retweeted_text".as("text"))
    //tw = tw.withColumn("u_mention", explode($"entities.user_mentions"))
    //tw = tw.withColumn("hts", explode($"entities.hashtags"))
    // 2x flatten
    tw
  }

  def referenceScenario0(): DataFrame = {
    var tw = loadTweets()
    tw = tw.select(
      $"retweeted_status.contributors".alias("r_contributors"),
      $"retweeted_status.created_at".alias("r_created_at"),
      $"retweeted_status.favorite_count".alias("r_favorite_count"),
      $"retweeted_status.favorited".alias("r_favorited"),
      $"retweeted_status.filter_level".alias("r_filter_level"),
      $"retweeted_status.id".alias("r_id"),
      $"retweeted_status.id_str".alias("r_id_str"),
      $"retweeted_status.in_reply_to_screen_name".alias("r_in_reply_to_screen_name"),
      $"retweeted_status.in_reply_to_status_id".alias("r_in_reply_to_status_id"),
      $"retweeted_status.in_reply_to_status_id_str".alias("r_in_reply_to_status_id_str"),
      $"retweeted_status.in_reply_to_user_id".alias("r_in_reply_to_user_id"),
      $"retweeted_status.in_reply_to_user_id_str".alias("r_in_reply_to_user_id_str"),
      $"retweeted_status.is_quote_status".alias("r_is_quote_status"),
      $"retweeted_status.lang".alias("r_lang"),
      $"retweeted_status.possibly_sensitive".alias("r_possibly_sensitive"),
      $"retweeted_status.quote_count".alias("r_quote_count"),
      $"retweeted_status.quoted_status_id".alias("r_quoted_status_id"),
      $"retweeted_status.quoted_status_id_str".alias("r_quoted_status_id_str"),
      $"retweeted_status.reply_count".alias("r_reply_count"),
      $"retweeted_status.retweet_count".alias("r_retweet_count"),
      $"retweeted_status.retweeted".alias("r_retweeted"),
      $"retweeted_status.source".alias("r_source"),
      $"retweeted_status.text".alias("r_text"),
      $"retweeted_status.truncated".alias("r_truncated"))/*,
      $"quoted_status.contributors".alias("q_contributors"),
      $"quoted_status.created_at".alias("q_created_at"),
      $"quoted_status.favorite_count".alias("q_favorite_count"),
      $"quoted_status.favorited".alias("q_favorited"),
      $"quoted_status.filter_level".alias("q_filter_level"),
      $"quoted_status.id".alias("q_id"),
      $"quoted_status.id_str".alias("q_id_str"),
      $"quoted_status.in_reply_to_screen_name".alias("q_in_reply_to_screen_name"),
      $"quoted_status.in_reply_to_status_id".alias("q_in_reply_to_status_id"),
      $"quoted_status.in_reply_to_status_id_str".alias("q_in_reply_to_status_id_str"),
      $"quoted_status.in_reply_to_user_id".alias("q_in_reply_to_user_id"),
      $"quoted_status.in_reply_to_user_id_str".alias("q_in_reply_to_user_id_str"),
      $"quoted_status.is_quote_status".alias("q_is_quote_status"),
      $"quoted_status.lang".alias("q_lang"),
      $"quoted_status.possibly_sensitive".alias("q_possibly_sensitive"),
      $"quoted_status.quote_count".alias("q_quote_count"),
      $"quoted_status.quoted_status_id".alias("q_quoted_status_id"),
      $"quoted_status.quoted_status_id_str".alias("q_quoted_status_id_str"),
      $"quoted_status.reply_count".alias("q_reply_count"),
      $"quoted_status.retweet_count".alias("q_retweet_count"),
      $"quoted_status.retweeted".alias("q_retweeted"),
      $"quoted_status.source".alias("q_source"),
      $"quoted_status.text".alias("q_text"),
      $"quoted_status.truncated".alias("q_truncated"))*/
    //tw = tw.filter($"r_retweeted" === true || $"q_quoted" === true)

    tw
  }

  def referenceScenario1(): DataFrame = {
    var tw = referenceScenario0()
    tw = tw.filter($"r_retweet_count" > 0)
    tw = tw.select(
      $"r_contributors".alias("contributors"),
      $"r_created_at".alias("created_at"),
      $"r_favorite_count".alias("favorite_count"),
      $"r_favorited".alias("favorited"),
      $"r_filter_level".alias("filter_level"),
      $"r_id".alias("id"),
      $"r_id_str".alias("id_str"),
      $"r_in_reply_to_screen_name".alias("in_reply_to_screen_name"),
      $"r_in_reply_to_status_id".alias("in_reply_to_status_id"),
      $"r_in_reply_to_status_id_str".alias("in_reply_to_status_id_str"),
      $"r_in_reply_to_user_id".alias("in_reply_to_user_id"),
      $"r_in_reply_to_user_id_str".alias("in_reply_to_user_id_str"),
      $"r_is_quote_status".alias("is_quote_status"),
      $"r_lang".alias("lang"),
      $"r_possibly_sensitive".alias("possibly_sensitive"),
      $"r_quote_count".alias("quote_count"),
      $"r_quoted_status_id".alias("quoted_status_id"),
      $"r_quoted_status_id_str".alias("quoted_status_id_str"),
      $"r_reply_count".alias("reply_count"),
      $"r_retweet_count".alias("retweet_count"),
      $"r_retweeted".alias("retweeted"),
      $"r_source".alias("source"),
      $"r_text".alias("text"),
      $"r_truncated".alias("truncated")
    )
    tw
  }

  def referenceScenario2(): DataFrame = {
    var tw = referenceScenario0()
    tw = tw.filter($"q_quote_count" > 0)
    tw = tw.select(
      $"q_contributors".alias("contributors"),
      $"q_created_at".alias("created_at"),
      $"q_favorite_count".alias("favorite_count"),
      $"q_favorited".alias("favorited"),
      $"q_filter_level".alias("filter_level"),
      $"q_id".alias("id"),
      $"q_id_str".alias("id_str"),
      $"q_in_reply_to_screen_name".alias("in_reply_to_screen_name"),
      $"q_in_reply_to_status_id".alias("in_reply_to_status_id"),
      $"q_in_reply_to_status_id_str".alias("in_reply_to_status_id_str"),
      $"q_in_reply_to_user_id".alias("in_reply_to_user_id"),
      $"q_in_reply_to_user_id_str".alias("in_reply_to_user_id_str"),
      $"q_is_quote_status".alias("is_quote_status"),
      $"q_lang".alias("lang"),
      $"q_possibly_sensitive".alias("possibly_sensitive"),
      $"q_quote_count".alias("quote_count"),
      $"q_quoted_status_id".alias("quoted_status_id"),
      $"q_quoted_status_id_str".alias("quoted_status_id_str"),
      $"q_reply_count".alias("reply_count"),
      $"q_retweet_count".alias("retweet_count"),
      $"q_retweeted".alias("retweeted"),
      $"q_source".alias("source"),
      $"q_text".alias("text"),
      $"q_truncated".alias("truncated")
    )
    tw
  }

  override def whyNotQuestion(): Twig = {
    var twig = new Twig()
    val root = twig.createNode("root")
    val rf = twig.createNode("text", 1, 1, "containsHe’ll never be the same")
    twig = twig.createEdge(root, rf)
    twig.validate().get
  }

  override def computeAlternatives(backtracedWhyNotQuestion: SchemaSubsetTree, input: LeafNode): PrimarySchemaSubsetTree = {
    val primaryTree = super.computeAlternatives(backtracedWhyNotQuestion, input)
    val saSize = 3 //testConfiguration.schemaAlternativeSize
    createAlternatives(primaryTree, saSize)

    replace1(primaryTree.alternatives(0).rootNode)
    replace2(primaryTree.alternatives(1).rootNode)
    replace21(primaryTree.alternatives(2).rootNode)
    primaryTree
  }

  def replace1(node: SchemaNode): Unit ={
    if (node.name == "retweeted_status") {
      node.name = "quoted_status"
      node.modified = true
      return
    }
    for (child <- node.children){
      replace1(child)
    }
  }

  def replace2(node:SchemaNode): Unit = {
    if (node.name == "retweeted_status" && node.parent.name == "root") {
      for (child <- node.children){
        if (child.name == "retweet_count"){
          child.name = "quote_count"
          child.modified = true
        }
      }
      return
    }
    for (child <- node.children){
      replace2(child)
    }
  }

  def replace21(node: SchemaNode): Unit ={
    if (node.name == "retweeted_status" && node.parent.name == "root") {
      node.name = "quoted_status"
      node.modified = true
      for (child <- node.children){
        if (child.name == "retweet_count"){
          child.name = "quote_count"
          child.modified = true
        }
      }
      return
    }
    for (child <- node.children){
      replace21(child)
    }
  }





}
