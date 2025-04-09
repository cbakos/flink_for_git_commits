package org.tudelft.bdp.flink

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.junit.Assert._
import org.junit.Test
import org.tudelft.bdp.flink.Protocol.{Commit, CommitGeo, CommitSummary}

class FlinkAssignmentTest {

  // Define an implicit Ordering for CommitSummary
  implicit val commitSummaryOrdering: Ordering[CommitSummary] = Ordering.by { commit: CommitSummary =>
    (commit.repo, commit.date, commit.amountOfCommits, commit.amountOfCommitters, commit.totalChanges, commit.mostPopularCommitter)
  }


  def setupEnv(): StreamExecutionEnvironment = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    env
  }

  def readCommits(env: StreamExecutionEnvironment): DataStream[Commit] = {
    env.readTextFile("data/flink_commits.json").map(new CommitParser)
  }

  def readGeoCommits(env: StreamExecutionEnvironment): DataStream[CommitGeo] = {
    env.readTextFile("data/flink_commits_geo.json").map(new CommitGeoParser)
  }

  def compareResults[T: Ordering](testSinkName1: String, testSinkName2: String): Unit = {
    val firstResult = TestSink.outputs[T](testSinkName1).toList.sorted
    val secondResult = TestSink.outputs[T](testSinkName2).toList.sorted

    assertEquals("Outputs should match", firstResult, secondResult)
  }


  @Test
  def testDummyQuestion(): Unit = {
    val env = setupEnv()
    val commitStream = readCommits(env)

    TestSink.clear()

    FlinkAssignmentExercise.dummy_question(commitStream)
      .addSink(new TestSink.CollectSink[String]("first"))
    FlinkAssignmentOldSolution.dummy_question(commitStream)
      .addSink(new TestSink.CollectSink[String]("second"))

    compareResults[String]("first", "second")
  }

  @Test
  def testQuestionOne(): Unit = {
    val env = setupEnv()
    val commitStream = readCommits(env)

    TestSink.clear()

    FlinkAssignmentExercise.question_one(commitStream)
      .addSink(new TestSink.CollectSink[String]("first"))
    FlinkAssignmentOldSolution.question_one(commitStream)
      .addSink(new TestSink.CollectSink[String]("second"))

    compareResults[String]("first", "second")
  }

  @Test
  def testQuestionTwo(): Unit = {
    val env = setupEnv()
    val commitStream = readCommits(env)

    TestSink.clear()

    FlinkAssignmentExercise.question_two(commitStream)
      .addSink(new TestSink.CollectSink[String]("first"))
    FlinkAssignmentOldSolution.question_two(commitStream)
      .addSink(new TestSink.CollectSink[String]("second"))

    compareResults[String]("first", "second")
  }

  @Test
  def testQuestionThree(): Unit = {
    val env = setupEnv()
    val commitStream = readCommits(env)

    TestSink.clear()

    FlinkAssignmentExercise.question_three(commitStream)
      .addSink(new TestSink.CollectSink[(String, Int)]("first"))
    FlinkAssignmentOldSolution.question_three(commitStream)
      .addSink(new TestSink.CollectSink[(String, Int)]("second"))

    compareResults[(String, Int)]("first", "second")
  }

  @Test
  def testQuestionFour(): Unit = {
    val env = setupEnv()
    val commitStream = readCommits(env)

    TestSink.clear()

    FlinkAssignmentExercise.question_four(commitStream)
      .addSink(new TestSink.CollectSink[(String, String, Int)]("first"))
    FlinkAssignmentOldSolution.question_four(commitStream)
      .addSink(new TestSink.CollectSink[(String, String, Int)]("second"))

    compareResults[(String, String, Int)]("first", "second")
  }

  @Test
  def testQuestionFive(): Unit = {
    val env = setupEnv()
    val commitStream = readCommits(env)

    TestSink.clear()

    FlinkAssignmentExercise.question_five(commitStream)
      .addSink(new TestSink.CollectSink[(String, Int)]("first"))
    FlinkAssignmentOldSolution.question_five(commitStream)
      .addSink(new TestSink.CollectSink[(String, Int)]("second"))

    compareResults[(String, Int)]("first", "second")
  }

  @Test
  def testQuestionSix(): Unit = {
    val env = setupEnv()
    val commitStream = readCommits(env)

    TestSink.clear()

    FlinkAssignmentExercise.question_six(commitStream)
      .addSink(new TestSink.CollectSink[(String, Int)]("first"))
    FlinkAssignmentOldSolution.question_six(commitStream)
      .addSink(new TestSink.CollectSink[(String, Int)]("second"))

    compareResults[(String, Int)]("first", "second")
  }

  @Test
  def testQuestionSeven(): Unit = {
    val env = setupEnv()
    val commitStream = readCommits(env)

    TestSink.clear()

    FlinkAssignmentExercise.question_seven(commitStream)
      .addSink(new TestSink.CollectSink[CommitSummary]("first"))
    FlinkAssignmentOldSolution.question_seven(commitStream)
      .addSink(new TestSink.CollectSink[CommitSummary]("second"))

    compareResults[CommitSummary]("first", "second")
  }

  @Test
  def testQuestionEight(): Unit = {
    val env = setupEnv()
    val commitStream = readCommits(env)
    val geoStream = readGeoCommits(env)

    TestSink.clear()

    FlinkAssignmentExercise.question_eight(commitStream, geoStream)
      .addSink(new TestSink.CollectSink[(String, Int)]("first"))
    FlinkAssignmentOldSolution.question_eight(commitStream, geoStream)
      .addSink(new TestSink.CollectSink[(String, Int)]("second"))

    compareResults[(String, Int)]("first", "second")
  }

  @Test
  def testQuestionNine(): Unit = {
    val env = setupEnv()
    val commitStream = readCommits(env)

    TestSink.clear()

    FlinkAssignmentExercise.question_nine(commitStream)
      .addSink(new TestSink.CollectSink[(String, String)]("first"))
    FlinkAssignmentOldSolution.question_nine(commitStream)
      .addSink(new TestSink.CollectSink[(String, String)]("second"))

    compareResults[(String, String)]("first", "second")
  }
}

