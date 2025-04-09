package org.tudelft.bdp.flink

import org.apache.flink.api.common.functions.MapFunction
import org.json4s.NoTypeHints
import org.json4s.ext.JavaTimeSerializers
import org.json4s.jackson.Serialization
import org.tudelft.bdp.flink.Protocol.Commit

/**
  * Parses a commit based on a JSON string.
  * You do NOT need to touch this file.
  */
class CommitParser extends MapFunction[String, Commit] {

  // Get deserialization formats.
  implicit lazy val formats = Serialization.formats(NoTypeHints) ++ JavaTimeSerializers.all

  /** Maps commit json to commit case class.*/
  override def map(value: String): Commit = {
    Serialization.read[Commit](value)
  }
}
