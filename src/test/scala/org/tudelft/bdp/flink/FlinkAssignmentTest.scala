package org.tudelft.bdp.flink

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.junit.Test
import org.junit.Assert.assertEquals

class FlinkAssignmentTest {

  @Test
  def testCompareTwoSolutionsOnCommitStream(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val commitStream =
      env
        .readTextFile("data/flink_commits.json")
        .map(new CommitParser)

    TestSink.clear()

    FlinkAssignmentExercise.dummy_question(commitStream)
      .addSink(new TestSink.CollectSink("first"))

    FlinkAssignmentOldSolution.dummy_question(commitStream)
      .addSink(new TestSink.CollectSink("second"))

    env.execute("Compare Solutions")

    val firstResult = TestSink.outputs("first").toList.sorted
    val secondResult = TestSink.outputs("second").toList.sorted

    assertEquals("Outputs should match", firstResult, secondResult)
  }
}
