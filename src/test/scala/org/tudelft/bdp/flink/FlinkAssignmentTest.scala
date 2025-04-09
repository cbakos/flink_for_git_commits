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

  def compareResults[T: Ordering](actualSinkName: String, expectedSinkName: String): Unit = {
    val actualResult = TestSink.outputs[T](actualSinkName).toList.sorted
    val expectedResult = TestSink.outputs[T](expectedSinkName).toList.sorted

    assertEquals("Outputs should match", expectedResult, actualResult)
  }


  @Test
  def testDummyQuestion(): Unit = {
    val env = setupEnv()
    val commitStream = readCommits(env)

    TestSink.clear()

    FlinkAssignmentExercise.dummy_question(commitStream)
      .addSink(new TestSink.CollectSink[String]("actual"))
    FlinkAssignmentOldSolution.dummy_question(commitStream)
      .addSink(new TestSink.CollectSink[String]("expected"))

    env.execute()
    compareResults[String]("actual", "expected")
  }

  @Test
  def testQuestionOne(): Unit = {
    val env = setupEnv()
    val commitStream = readCommits(env)

    TestSink.clear()

    FlinkAssignmentExercise.question_one(commitStream)
      .addSink(new TestSink.CollectSink[String]("actual"))
    FlinkAssignmentOldSolution.question_one(commitStream)
      .addSink(new TestSink.CollectSink[String]("expected"))

    env.execute()
    compareResults[String]("actual", "expected")
  }

  @Test
  def testQuestionTwo(): Unit = {
    val env = setupEnv()
    val commitStream = readCommits(env)

    TestSink.clear()

    FlinkAssignmentExercise.question_two(commitStream)
      .addSink(new TestSink.CollectSink[String]("actual"))
    FlinkAssignmentOldSolution.question_two(commitStream)
      .addSink(new TestSink.CollectSink[String]("expected"))

    env.execute()
    compareResults[String]("actual", "expected")
  }

  @Test
  def testQuestionThree(): Unit = {
    val env = setupEnv()
    val commitStream = readCommits(env)

    TestSink.clear()

    FlinkAssignmentExercise.question_three(commitStream)
      .addSink(new TestSink.CollectSink[(String, Int)]("actual"))
    FlinkAssignmentOldSolution.question_three(commitStream)
      .addSink(new TestSink.CollectSink[(String, Int)]("expected"))

    env.execute()
    compareResults[(String, Int)]("actual", "expected")
  }

  @Test
  def testQuestionFour(): Unit = {
    val env = setupEnv()
    val commitStream = readCommits(env)

    TestSink.clear()

    FlinkAssignmentExercise.question_four(commitStream)
      .addSink(new TestSink.CollectSink[(String, String, Int)]("actual"))
    FlinkAssignmentOldSolution.question_four(commitStream)
      .addSink(new TestSink.CollectSink[(String, String, Int)]("expected"))

    env.execute()
    compareResults[(String, String, Int)]("actual", "expected")
  }

  @Test
  def testQuestionFive(): Unit = {
    val env = setupEnv()
    val commitStream = readCommits(env)

    TestSink.clear()

    FlinkAssignmentExercise.question_five(commitStream)
      .addSink(new TestSink.CollectSink[(String, Int)]("actual"))
    FlinkAssignmentOldSolution.question_five(commitStream)
      .addSink(new TestSink.CollectSink[(String, Int)]("expected"))

    env.execute()
    compareResults[(String, Int)]("actual", "expected")
  }

  @Test
  def testQuestionSix(): Unit = {
    val env = setupEnv()
    val commitStream = readCommits(env)

    TestSink.clear()

    FlinkAssignmentExercise.question_six(commitStream)
      .addSink(new TestSink.CollectSink[(String, Int)]("actual"))
    FlinkAssignmentOldSolution.question_six(commitStream)
      .addSink(new TestSink.CollectSink[(String, Int)]("expected"))

    env.execute()
    compareResults[(String, Int)]("actual", "expected")
  }

  @Test
  def testQuestionSeven(): Unit = {
    val env = setupEnv()
    val commitStream = readCommits(env)

    TestSink.clear()

    FlinkAssignmentExercise.question_seven(commitStream)
      .addSink(new TestSink.CollectSink[CommitSummary]("actual"))
    FlinkAssignmentOldSolution.question_seven(commitStream)
      .addSink(new TestSink.CollectSink[CommitSummary]("expected"))

    env.execute()
    compareResults[CommitSummary]("actual", "expected")
  }

  @Test
  def testQuestionEight(): Unit = {
    val env = setupEnv()
    val commitStream = readCommits(env)
    val geoStream = readGeoCommits(env)

    TestSink.clear()

    FlinkAssignmentExercise.question_eight(commitStream, geoStream)
      .addSink(new TestSink.CollectSink[(String, Int)]("actual"))
    FlinkAssignmentOldSolution.question_eight(commitStream, geoStream)
      .addSink(new TestSink.CollectSink[(String, Int)]("expected"))

    env.execute()
    compareResults[(String, Int)]("actual", "expected")
  }

  @Test
  def testQuestionNine(): Unit = {
    val env = setupEnv()
    val commitStream = readCommits(env)

    TestSink.clear()

    FlinkAssignmentExercise.question_nine(commitStream)
      .addSink(new TestSink.CollectSink[(String, String)]("actual"))
    FlinkAssignmentOldSolution.question_nine(commitStream)
      .addSink(new TestSink.CollectSink[(String, String)]("expected"))

    env.execute()
    compareResults[(String, String)]("actual", "expected")
  }
}

