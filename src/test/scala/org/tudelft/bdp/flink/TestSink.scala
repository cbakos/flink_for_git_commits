package org.tudelft.bdp.flink

import org.apache.flink.streaming.api.functions.sink.SinkFunction
import scala.collection.mutable

object TestSink {
  private val collectedOutputs: mutable.Map[String, mutable.ListBuffer[Any]] = mutable.Map()

  def clear(): Unit = synchronized {
    collectedOutputs.clear()
  }

  def outputs[T](name: String): Seq[T] = synchronized {
    collectedOutputs.getOrElse(name, Seq.empty).asInstanceOf[Seq[T]]
  }

  class CollectSink[T](val name: String) extends SinkFunction[T] with Serializable {
    override def invoke(value: T, context: SinkFunction.Context[_]): Unit = synchronized {
      val buffer = collectedOutputs.getOrElseUpdate(name, mutable.ListBuffer()).asInstanceOf[mutable.ListBuffer[T]]
      buffer += value
    }
  }
}
