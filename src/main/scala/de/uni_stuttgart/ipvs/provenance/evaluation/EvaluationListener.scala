package de.uni_stuttgart.ipvs.provenance.evaluation

import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd, SparkListenerApplicationStart, SparkListenerJobEnd, SparkListenerJobStart, SparkListenerStageCompleted}
import org.slf4j.LoggerFactory

class EvaluationListener extends SparkListener {

  lazy val logger = LoggerFactory.getLogger(getClass)

  val jobStartTimes = scala.collection.mutable.Map.empty[Int, Long]

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    logger.warn(s"Stage ${stageCompleted.stageInfo.name} completed, runTime: ${stageCompleted.stageInfo.taskMetrics.executorRunTime}, " +
      s"cpuTime: ${stageCompleted.stageInfo.taskMetrics.executorCpuTime}")
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    jobStartTimes += (jobStart.jobId -> jobStart.time)
    logger.warn(s"Job ${jobStart.jobId} started at: ${jobStart.time}")
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    logger.warn(s"Job ${jobEnd.jobId} completed at: ${jobEnd.time}")
    val startTime = jobStartTimes.getOrElse(jobEnd.jobId, 0L)
    logger.warn(s"Job ${jobEnd.jobId} exeuction time: ${jobEnd.time - startTime}")
  }

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    logger.warn(s"Application ${applicationStart.appName} started, runTime: ${applicationStart.time}")
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    logger.warn(s"Application completed, runTime: ${applicationEnd.time}")
  }

  //TODO: Add git commit here
  //TODO: Add date and time
  //TODO: Create a repository for results
  //TODO: integrate Spark, Hadoop, Scala and Java version
  //

}
