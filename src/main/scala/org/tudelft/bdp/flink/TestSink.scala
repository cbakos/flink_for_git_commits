package org.tudelft.bdp.flink

import org.apache.flink.streaming.api.functions.sink.SinkFunction
import scala.collection.mutable

object TestSink {
  private val collectedOutputs: mutable.Map[String, mutable.ListBuffer[String]] = mutable.Map()

  def clear(): Unit = synchronized {
    collectedOutputs.clear()
  }

  def outputs(name: String): Seq[String] = synchronized {
    collectedOutputs.getOrElse(name, Seq.empty)
  }

  class CollectSink(val name: String) extends SinkFunction[String] with Serializable {
    override def invoke(value: String, context: SinkFunction.Context[_]): Unit = synchronized {
      val buffer = collectedOutputs.getOrElseUpdate(name, mutable.ListBuffer())
      buffer += value
    }
  }
}
