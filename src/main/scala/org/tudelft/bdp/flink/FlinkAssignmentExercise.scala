package org.tudelft.bdp.flink

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
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
   * repo: name of the repo.
   * date: use the start of the window in format "dd-MM-yyyy".
   * amountOfCommits: the number of commits on that day for that repository.
   * amountOfCommitters: the amount of unique committers contributing to the repository.
   * totalChanges: the sum of total changes in all commits.
   * topCommitter: the top committer of that day i.e. with the most commits. Note: if there are multiple top committers; create a comma separated string sorted alphabetically e.g. `georgios,jeroen,wouter`
   *
   * Hint: Write your own ProcessWindowFunction.
   * Output format: CommitSummary
   */
  def question_seven(commitStream: DataStream[Commit]): DataStream[CommitSummary] = ???

  /**
   * For this exercise there is another dataset containing CommitGeo events. A CommitGeo event stores the sha of a commit, a date and the continent it was produced in.
   * You can assume that for every commit there is a CommitGeo event arriving within a timeframe of 1 hour before and 30 minutes after the commit.
   * Get the weekly amount of changes for the java files (.java extension) per continent.
   *
   * Hint: Find the correct join to use!
   * Output format: (continent, amount)
   */
  def question_eight(commitStream: DataStream[Commit], geoStream: DataStream[CommitGeo]): DataStream[(String, Int)] = ???

  /**
   * Find all files that were added and removed within one day. Output as (repository, filename).
   *
   * Hint: Use the Complex Event Processing library (CEP).
   * Output format: (repository, filename)
   */
  def question_nine(inputStream: DataStream[Commit]): DataStream[(String, String)] = ???

}