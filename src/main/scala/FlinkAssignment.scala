import java.text.SimpleDateFormat

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.cep.functions.PatternProcessFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import util.Protocol.{Commit, CommitGeo, CommitSummary, File, Stats}
import util.{CommitGeoParser, CommitParser}
import java.util

import scala.collection.mutable.ListBuffer

/** Do NOT rename this class, otherwise autograding will fail. **/
object FlinkAssignment {

  val env = StreamExecutionEnvironment.getExecutionEnvironment

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
    //dummy_question(commitStream).print()
    //question_seven(commitStream).print()
    question_six(commitStream)
      .print()

    /** Start the streaming environment. **/
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
    input.filter(c => c.stats.isDefined)
      .filter(c => c.stats.get.additions >= 20)
      .map(c => c.sha)
  }

  /**
    * Write a Flink application which outputs the names of the files with more than 30 deletions.
    * Output format:  fileName
    */
  def question_two(input: DataStream[Commit]): DataStream[String] = {
    input
      .flatMap(c =>
        c.files.filter(f => f.deletions > 30)
          .filter(f => f.filename.isDefined)
          .map(f => f.filename.get))

  }

  /**
    * Count the occurrences of Java and Scala files. I.e. files ending with either .scala or .java.
    * Output format: (fileExtension, #occurrences)
    */
  def question_three(input: DataStream[Commit]): DataStream[(String, Int)] = {
    input.flatMap(c =>
      c.files.filter(f => f.filename.isDefined)
        .filter(f => f.filename.get.endsWith(".scala") || f.filename.get.endsWith(".java"))
        .map(f => (f.filename.get.split("\\.").repr.last, 1))).keyBy(t => t._1)
      .mapWithState((in: (String, Int), count: Option[(Int, Int)]) =>
        (in._1, count) match {
          case ("scala", Some(c)) => ((in._1, c._1 + 1), Some((c._1 + 1, c._2)))
          case ("java", Some(c)) => ((in._1, c._2 + 1), Some((c._1, c._2 + 1)))
          case ("scala", None) => ((in._1, 1), Some((1, 0)))
          case ("java", None) => ((in._1, 1), Some((0, 1)))
          //case(_,_) => throw new Exception("something went wrong")
        }).map(a => (a._1, a._2))
  }

  /**
    * Count the total amount of changes for each file status (e.g. modified, removed or added) for the following extensions: .js and .py.
    * Output format: (extension, status, count)
    */
  def question_four(input: DataStream[Commit]): DataStream[(String, String, Int)] = {
    def addOrUpdate(m: collection.mutable.Map[(String, String), Int], k: (String, String), kv: ((String, String), Int)): collection.mutable.Map[(String, String), Int] = {
      m.get(k) match {
        case Some(e) => {
          m.update(k, e + kv._2)
          m
        }
        case None => {
          m += kv
          m
        }
      }
    }

    input.flatMap(c =>
      c.files.filter(f => f.filename.isDefined && f.status.isDefined)
        .filter(f => f.filename.get.endsWith(".js") || f.filename.get.endsWith(".py"))
        .map(f => (f.filename.get.split("\\.").repr.last, f.status.get, f.changes))).keyBy(t => (t._1, t._2))
      .mapWithState((in: (String, String, Int), count: Option[collection.mutable.Map[(String, String), Int]]) =>
        (in._1, in._2, count) match {
          case (ext, status, Some(c)) => ((in._1, in._2, in._3 + c.getOrElse((ext, status), 0)), Option(addOrUpdate(c, (ext, status), (ext, status) -> in._3)))
          case (_, _, None) => ((in._1, in._2, in._3), Option(collection.mutable.Map((in._1, in._2) -> in._3)))
        })
  }


  /**
    * For every day output the amount of commits. Include the timestamp in the following format dd-MM-yyyy; e.g. (26-06-2019, 4) meaning on the 26th of June 2019 there were 4 commits.
    * Make use of a non-keyed window.
    * Output format: (date, count)
    */
  def question_five(input: DataStream[Commit]): DataStream[(String, Int)] = {
    val dateFormat = new SimpleDateFormat("dd-MM-yyyy")
    //(dateFormat.format(c.commit.committer.date)
    input.map(c => (c.commit.committer.date, 1))
      .assignAscendingTimestamps(t => t._1.getTime)
      .map(t => (dateFormat.format(t._1), t._2))
      .windowAll(TumblingEventTimeWindows.of(Time.days(1)))
      .sum(1)


  }

  /**
    * Consider two types of commits; small commits and large commits whereas small: 0 <= x <= 20 and large: x > 20 where x = total amount of changes.
    * Compute every 12 hours the amount of small and large commits in the last 48 hours.
    * Output format: (type, count)
    */
  def question_six(input: DataStream[Commit]): DataStream[(String, Int)] = {
    input
      .assignAscendingTimestamps(c => c.commit.committer.date.getTime)
      .filter(c => c.stats.isDefined)
      .map(c => {
        if (c.stats.get.total <= 20) {
          ("small", 1)
        } else {
          ("large", 1)
        }
      })
      .keyBy(0)
      .window(SlidingEventTimeWindows.of(Time.hours(48), Time.hours(12)))
      .sum(1)
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
  def question_seven(commitStream: DataStream[Commit]): DataStream[CommitSummary] = {
    val dateFormat = new SimpleDateFormat("dd-MM-yyyy")
    commitStream
      .assignAscendingTimestamps(c => c.commit.committer.date.getTime)
      .filter(c => c.stats.isDefined)
      .map(c => {
        (c.url.split("/").apply(4) + "/" + c.url.split("/").apply(5), dateFormat.format(c.commit.committer.date), 1, c.commit.committer.name, c.stats.get.total, "")
      })
      .keyBy(x => x._1)
      .window(TumblingEventTimeWindows.of(Time.days(1)))
      .process(new MyProcessWindowFunction())
      .filter(cs => cs.amountOfCommits > 20)
      .filter(cs => cs.amountOfCommitters <= 2)
  }

  private class MyProcessWindowFunction extends ProcessWindowFunction[(String, String, Int, String, Int, String), CommitSummary, String, TimeWindow] {

    override def process(key: String, context: Context, input: Iterable[(String, String, Int, String, Int, String)], out: Collector[CommitSummary]): Unit = {
      var repo = ""
      var date = ""
      var amountOfCommits = 0
      var totalChanges = 0
      var topCommitter = new ListBuffer[String]()
      var committers = scala.collection.mutable.Map[String, Int]()
      var uniqueCommitters = 0
      for (in <- input) {
        repo = in._1
        date = in._2
        amountOfCommits = amountOfCommits + 1
        totalChanges = totalChanges + in._5
        if (committers.contains(in._4)) {
          var curV: Int = committers.get(in._4).get
          committers(in._4) = curV + 1
        } else {
          committers(in._4) = 1
          uniqueCommitters = uniqueCommitters + 1
        }

      }
      var topNumCommits = -1
      for ((k, v) <- committers) {
        if (topNumCommits < v) {
          topCommitter = new ListBuffer[String]()
          topCommitter += k
          topNumCommits = v
        } else if (topNumCommits == v) {
          topCommitter += k
        }
      }
      var topCommitterString = ""
      var topCommitterSorted: List[String] = topCommitter.sorted.toList
      for (c <- topCommitterSorted) {
        topCommitterString += c
        topCommitterString += ","
      }
      topCommitterString = topCommitterString.substring(0, topCommitterString.length - 1)

      out.collect(CommitSummary(repo, date, amountOfCommits, uniqueCommitters, totalChanges, topCommitterString))
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
      .assignAscendingTimestamps(c => c.commit.committer.date.getTime)
      .keyBy(c => c.sha)
      .intervalJoin(geoStream
        .assignAscendingTimestamps(c => c.createdAt.getTime)
        .keyBy(c => c.sha))
      .between(Time.minutes(-60), Time.minutes(30))
      .process(new ProcessJoinFunction[Commit, CommitGeo, (List[File], String, Long)] {
        override def processElement(left: Commit, right: CommitGeo, ctx: ProcessJoinFunction[Commit, CommitGeo, (List[File], String, Long)]#Context, out: Collector[(List[File], String, Long)]): Unit = {
          out.collect((left.files, right.continent, left.commit.committer.date.getTime))
        }
      })
      .map(t=>(t._2, t._1.foldLeft(0)((acc, f: File) => {
      if (f.filename.isDefined && f.filename.get.endsWith(".java")) acc + f.changes else acc
    })))
      .keyBy(t=>t._1)
      .window(TumblingEventTimeWindows.of(Time.days(7)))
      .reduce{
        (a,b)=> (a._1, a._2+b._2)
      }
      .filter(t=>t._2>0)
  }

/**
* Find all files that were added and removed within one day. Output as (repository, filename).
*
* Hint: Use the Complex Event Processing library (CEP).
* Output format: (repository, filename)
*/
def question_nine(inputStream: DataStream[Commit]): DataStream[(String, String)] = {
 val input =
   inputStream
    .assignAscendingTimestamps(c=>c.commit.committer.date.getTime)
    .map(c=>(c.url.split("/").apply(4) + "/" + c.url.split("/").apply(5), c.files))
    .map(t=>t._2.map(f=>(t._1, f)))
   .flatMap(l=>l)
   .filter(t=>t._2.status.isDefined && (t._2.status.get == "added" || t._2.status.get == "removed") && t._2.filename.isDefined)
   .keyBy(t=>(t._1, t._2.filename.get))


  val pattern = Pattern.begin[(String, File)]("start")
    .where(t=>t._2.status.get == "added")
    .followedBy("end")
    .where(t=>t._2.status.get == "removed")
    .within(Time.days(1))

  val patternStream = CEP.pattern(input, pattern)
  patternStream.process(
    new PatternProcessFunction[(String, File), (String, String)]() {
      override def processMatch(
                                 `match`: java.util.Map[String, java.util.List[(String, File)]],
                                 ctx: PatternProcessFunction.Context,
                                 out: Collector[(String, String)]): Unit = {
        val a = `match`.get("start").get(0)._1
        val b = `match`.get("start").get(0)._2.filename.get
        out.collect(a,b)

      }
    })
}
}

