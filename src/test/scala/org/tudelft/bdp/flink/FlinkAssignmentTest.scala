package org.tudelft.bdp.flink

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.junit.Assert._
import org.junit.Test
import org.tudelft.bdp.flink.Protocol.{Commit, CommitGeo, CommitSummary}

class FlinkAssignmentTest {

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

  @Test
  def testDummyQuestionFull(): Unit = {
    val env = setupEnv()
    val commitStream = readCommits(env)

    TestSink.clear()

    FlinkAssignmentExercise.dummy_question(commitStream)
      .addSink(new TestSink.CollectSink("first"))

    FlinkAssignmentOldSolution.dummy_question(commitStream)
      .addSink(new TestSink.CollectSink("second"))

    val firstResult = TestSink.outputs("first").toList.sorted
    val secondResult = TestSink.outputs("second").toList.sorted

    assertEquals("Outputs should match", firstResult, secondResult)
  }

  @Test
  def testQuestionOne(): Unit = {
    val env = setupEnv()
    val commitStream = readCommits(env)

    TestSink.clear()

    FlinkAssignmentExercise.question_one(commitStream)
      .addSink(new TestSink.CollectSink("q1"))

    env.execute("Test Question 1")

    val result = TestSink.outputs("q1").toList.sorted

    val expected = List(  // replace with actual sha values expected
      "abc123", "def456"
    ).sorted

    assertEquals(expected, result)
  }

  @Test
  def testQuestionTwo(): Unit = {
    val env = setupEnv()
    val commitStream = readCommits(env)

    TestSink.clear()

    FlinkAssignmentExercise.question_two(commitStream)
      .addSink(new TestSink.CollectSink("q2"))

    env.execute("Test Question 2")

    val result = TestSink.outputs("q2").toList.sorted
    val expected = List("src/Main.scala", "README.md").sorted  // adjust as needed
    assertEquals(expected, result)
  }

  @Test
  def testQuestionThree(): Unit = {
    val env = setupEnv()
    val commitStream = readCommits(env)

    TestSink.clear()

    FlinkAssignmentExercise.question_three(commitStream)
      .map(x => s"${x._1}:${x._2}")
      .addSink(new TestSink.CollectSink("q3"))

    env.execute("Test Question 3")

    val result = TestSink.outputs("q3").toList.sorted
    val expected = List("scala:12", "java:8").sorted
    assertEquals(expected, result)
  }

  @Test
  def testQuestionFour(): Unit = {
    val env = setupEnv()
    val commitStream = readCommits(env)

    TestSink.clear()

    FlinkAssignmentExercise.question_four(commitStream)
      .map { t: (String, String, Int) =>
        val (ext, status, count) = t
        s"$ext|$status|$count"
      }

      .addSink(new TestSink.CollectSink("q4"))

    env.execute("Test Question 4")

    val result = TestSink.outputs("q4").toList.sorted
    val expected = List("js|added|3", "py|modified|6").sorted
    assertEquals(expected, result)
  }

  @Test
  def testQuestionFive(): Unit = {
    val env = setupEnv()
    val commitStream = readCommits(env)

    TestSink.clear()

    FlinkAssignmentExercise.question_five(commitStream)
      .map { t: (String, Int) =>
        val (date, count) = t
        s"$date:$count"
      }

      .addSink(new TestSink.CollectSink("q5"))

    env.execute("Test Question 5")

    val result = TestSink.outputs("q5").toList.sorted
    val expected = List("26-06-2019:4", "27-06-2019:2").sorted
    assertEquals(expected, result)
  }

  @Test
  def testQuestionSix(): Unit = {
    val env = setupEnv()
    val commitStream = readCommits(env)

    TestSink.clear()

    FlinkAssignmentExercise.question_six(commitStream)
      .map { t: (String, Int) =>
        val (tpe, count) = t
        s"$tpe:$count"
      }

      .addSink(new TestSink.CollectSink("q6"))

    env.execute("Test Question 6")

    val result = TestSink.outputs("q6").toList.sorted
    val expected = List("small:12", "large:3").sorted
    assertEquals(expected, result)
  }

  @Test
  def testQuestionSeven(): Unit = {
    val env = setupEnv()
    val commitStream = readCommits(env)

    TestSink.clear()

    FlinkAssignmentExercise.question_seven(commitStream)
      .map { summary: CommitSummary =>
        s"${summary.repo},${summary.date},${summary.amountOfCommits},${summary.amountOfCommitters},${summary.totalChanges},${summary.mostPopularCommitter}"
      }
      .addSink(new TestSink.CollectSink("q7"))

    env.execute("Test Question 7")

    val result = TestSink.outputs("q7").toList.sorted
    val expected = List(
      "apache/flink,26-06-2019,21,2,234,georgios,jeroen"
    ).sorted
    assertEquals(expected, result)
  }

  @Test
  def testQuestionEight(): Unit = {
    val env = setupEnv()
    val commitStream = readCommits(env)
    val geoStream = readGeoCommits(env)

    TestSink.clear()

    FlinkAssignmentExercise.question_eight(commitStream, geoStream)
      .map { (tuple: (String, Int)) =>
        val (continent, amount) = tuple
        s"$continent:$amount"
      }
      .addSink(new TestSink.CollectSink("q8"))


    env.execute("Test Question 8")

    val result = TestSink.outputs("q8").toList.sorted
    val expected = List("Europe:45", "Asia:12").sorted
    assertEquals(expected, result)
  }

  @Test
  def testQuestionNine(): Unit = {
    val env = setupEnv()
    val commitStream = readCommits(env)

    TestSink.clear()

    FlinkAssignmentExercise.question_nine(commitStream)
      .map { (tuple: (String, String)) =>
        val (repo, file) = tuple
        s"$repo:$file"
      }
      .addSink(new TestSink.CollectSink("q9"))

    env.execute("Test Question 9")

    val result = TestSink.outputs("q9").toList.sorted
    val expected = List("apache/flink|src/main/App.scala").sorted
    assertEquals(expected, result)
  }
}

