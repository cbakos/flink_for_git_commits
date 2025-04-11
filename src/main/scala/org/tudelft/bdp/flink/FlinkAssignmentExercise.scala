package org.tudelft.bdp.flink

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import org.tudelft.bdp.flink.Protocol.{Commit, CommitGeo, CommitSummary}

/** Do NOT rename this class, otherwise autograding will fail. **/
object FlinkAssignmentExercise {

  private val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  def main(args: Array[String]): Unit = {

    /**
     * Setups the streaming environment including loading and parsing of the datasets.
     *
     * DO NOT TOUCH!
     */
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // Read and parses commit stream.
    val commitStream =
      env
        .readTextFile("data/flink_commits.json")
        .map(new CommitParser)

    // Read and parses commit geo stream.
    val commitGeoStream =
      env
        .readTextFile("data/flink_commits_geo.json")
        .map(new CommitGeoParser)

    /** Use the space below to print and test your questions. */
    dummy_question(commitStream).print()

    /** Start the streaming environment. * */
    env.execute()
  }


  /** Dummy question which maps each commits to its SHA. */
  def dummy_question(input: DataStream[Commit]): DataStream[String] = {
    input.map(_.sha)
  }

  /**
   * Write a Flink application which outputs the sha of commits with at least 20 additions.
   * Output format: sha
   */
  def question_one(input: DataStream[Commit]): DataStream[String] = {
    input
      .filter(_.stats.isDefined)
      .filter(_.stats.get.additions >= 20)
      .map(_.sha)
  }

  /**
   * Write a Flink application which outputs the names of the files with more than 30 deletions.
   * Output format:  fileName
   */
  def question_two(input: DataStream[Commit]): DataStream[String] = {
    input
      .flatMap(_.files)
      .filter(file => file.filename.isDefined && file.deletions > 30)
      .map(_.filename.get)
  }

  /**
   * Count the occurrences of Java and Scala files. I.e. files ending with either .scala or .java.
   * Output format: (fileExtension, #occurrences)
   */
  def question_three(input: DataStream[Commit]): DataStream[(String, Int)] = {
    input
      .flatMap(_.files)
      .filter(_.filename.isDefined)
      .map(_.filename.get)
      .filter(file => file.endsWith(".scala") || file.endsWith(".java"))
      .map(file => (file.split("\\.").last, 1))
      .keyBy(extension => extension._1)
      .reduce((x, y) => (x._1, x._2 + y._2))
  }

  /**
   * Count the total amount of changes for each file status (e.g. modified, removed or added) for the following extensions: .js and .py.
   * Output format: (extension, status, count)
   */
  def question_four(input: DataStream[Commit]): DataStream[(String, String, Int)] = {
    input
      .flatMap(_.files)
      .filter(file => file.filename.isDefined && file.status.isDefined)
      .map(file => (file.filename.get, file.status.get, file.changes))
      .filter(file => file._1.endsWith(".js") || file._1.endsWith(".py"))
      .map(x => (x._1.split("\\.").last, x._2, x._3))
      .keyBy(x => (x._1, x._2))
      .reduce((x, y) => (x._1, x._2, x._3 + y._3))
  }

  /**
   * For every day output the amount of commits. Include the timestamp in the following format dd-MM-yyyy; e.g. (26-06-2019, 4) meaning on the 26th of June 2019 there were 4 commits.
   * Make use of a non-keyed window.
   * Output format: (date, count)
   */
  def question_five(input: DataStream[Commit]): DataStream[(String, Int)] = {
    val dateFormat = new java.text.SimpleDateFormat("dd-MM-yyyy")
    input
      .assignAscendingTimestamps(_.commit.committer.date.getTime)
      .map(x => (x.commit.committer.date, 1))
      .map(x => (dateFormat.format(x._1), x._2))
      .windowAll(TumblingEventTimeWindows.of(Time.days(1)))
      .reduce((x, y) => (x._1, x._2 + y._2))
  }

  /**
   * Consider two types of commits; small commits and large commits whereas small: 0 <= x <= 20 and large: x > 20 where x = total amount of changes.
   * Compute every 12 hours the amount of small and large commits in the last 48 hours.
   * Output format: (type, count)
   */
  def question_six(input: DataStream[Commit]): DataStream[(String, Int)] = {
    input
      .assignAscendingTimestamps(_.commit.committer.date.getTime)
      .filter(_.stats.isDefined)
      .map(_.stats.get.total)
      .map(cnt => (if (cnt > 20) "large" else "small", 1))
      .keyBy(_._1)
      .window(SlidingEventTimeWindows.of(Time.hours(48), Time.hours(12)))
      .reduce((x, y) => (x._1, x._2 + y._2))
  }

  /**
   * For each repository compute a daily commit summary and output the summaries with more than 20 commits and at most 2 unique committers. The CommitSummary case class is already defined.
   *
   * The fields of this case class:
   *
   * repo: name of the repo (including username for unique identification)
   * date: use the start of the window in format "dd-MM-yyyy".
   * amountOfCommits: the number of commits on that day for that repository.
   * amountOfCommitters: the amount of unique committers contributing to the repository.
   * totalChanges: the sum of total changes in all commits.
   * topCommitter: the top committer of that day i.e. with the most commits. Note: if there are multiple top committers; create a comma separated string sorted alphabetically e.g. `georgios,jeroen,wouter`
   *
   * Hint: Write your own ProcessWindowFunction.
   * Output format: CommitSummary
   */
  def question_seven(commitStream: DataStream[Commit]): DataStream[CommitSummary] = {
    val dateFormat = new java.text.SimpleDateFormat("dd-MM-yyyy")
    commitStream
      .assignAscendingTimestamps(_.commit.committer.date.getTime)
      .filter(_.stats.isDefined)
      .map(
        x => (x.url.split("/")(4) + "/" + x.url.split("/")(5),
        dateFormat.format(x.commit.committer.date),
        x.stats.get.total,
        x.commit.committer.name))
      .keyBy(x => (x._1, x._2))
      .window(TumblingEventTimeWindows.of(Time.days(1)))
      .aggregate(new CommitSummaryAggregate())
      .filter(x => (x.amountOfCommits > 20 && x.amountOfCommitters <= 2))
  }

  /**
   * I use an AggregateFunction approach over ProcessWindowFunction for better performance: this way, we can have
   * incremental window processing, no need to wait for the window to finish to begin processing it.
   * IN: (repo: String, date: String, numChangesInCommit: Int, committerName: String
   * ACC: (repo: String, date: String, amountOfCommits: Int, committersMap (committerName -> numOfCommits): Map[String, Int], totalChanges: Int)
   * OUT: CommitSummary
   */
  private class CommitSummaryAggregate extends AggregateFunction[(String, String, Int, String), (String, String, Int, Map[String, Int], Int), CommitSummary] {

    override def createAccumulator(): (String, String, Int, Map[String, Int], Int) = {
      ("", "", 0, Map[String, Int](), 0)
    }

    override def add(in: (String, String, Int, String), acc: (String, String, Int, Map[String, Int], Int)): (String, String, Int, Map[String, Int], Int) = {
      var committersMap: Map[String, Int] = Map[String, Int]()
      if (acc._4.contains(in._4)) {
        committersMap = acc._4 + (in._4 -> (acc._4(in._4) + 1))
      } else {
        committersMap = acc._4 + (in._4 -> 1)
      }
      (in._1, in._2, acc._3 + 1, committersMap, acc._5 + in._3)
    }

    override def getResult(acc: (String, String, Int, Map[String, Int], Int)): CommitSummary = {
      val maxCommits = acc._4.values.max
      val topCommitter = acc._4
        .filter { case (_, v) => v == maxCommits }   // get all top committers
        .keys
        .toList
        .sorted                                      // alphabetical order
        .mkString(",")                               // comma-separated
      CommitSummary(acc._1, acc._2, acc._3, acc._4.size, acc._5, topCommitter)
    }

    override def merge(acc: (String, String, Int, Map[String, Int], Int), acc1: (String, String, Int, Map[String, Int], Int)): (String, String, Int, Map[String, Int], Int) = {
      val combinedCommittersMap = acc1._4.foldLeft(acc._4) {
        case (acc, (k, v)) =>
          acc + (k -> acc.get(k).map(_ + v).getOrElse(v))  // sum values if key exists
      }
      (acc._1, acc._2, acc._3 + acc1._3, combinedCommittersMap, acc._5 + acc1._5)
    }
  }

  /**
   * For this exercise there is another dataset containing CommitGeo events. A CommitGeo event stores the sha of a commit, a date and the continent it was produced in.
   * You can assume that for every commit there is a CommitGeo event arriving within a timeframe of 1 hour before and 30 minutes after the commit.
   * Get the weekly amount of changes for the java files (.java extension) per continent.
   *
   * Hint: Find the correct join to use!
   * Output format: (continent, amount)
   */
  def question_eight(commitStream: DataStream[Commit], geoStream: DataStream[CommitGeo]): DataStream[(String, Int)] = {
    commitStream
      .assignAscendingTimestamps(_.commit.committer.date.getTime)
      .flatMap {c => c.files.map(f => (c.sha, f))}
      .filter(_._2.filename.isDefined)
      .map(x => (x._1, x._2.filename.get, x._2.changes))
      .filter(_._2.endsWith(".java"))
      .map(x => (x._1, x._3))
      .keyBy(_._1)
      .intervalJoin(geoStream.assignAscendingTimestamps(_.createdAt.getTime).keyBy(_.sha))
      .between(Time.minutes(-60), Time.minutes(30))
      .process((in1: (String, Int), in2: CommitGeo, _: ProcessJoinFunction[(String, Int), CommitGeo, (String, Int)]#Context, collector: Collector[(String, Int)]) => {
        collector.collect((in2.continent, in1._2))
      })
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.days(7)))
      .reduce((x, y) => (x._1, x._2 + y._2))
  }

  /**
   * Find all files that were added and removed within one day. Output as (repository, filename).
   *
   * Hint: Use the Complex Event Processing library (CEP).
   * Output format: (repository, filename)
   */
  def question_nine(inputStream: DataStream[Commit]): DataStream[(String, String)] = ???

}