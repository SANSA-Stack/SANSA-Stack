package net.sansa_stack.rdf.common.kryo.jena

import java.nio.ByteBuffer

import com.esotericsoftware.kryo.io.{Input, Output}

/* Not used anymore
object ByteBufferUtils {
  def writeRemaining(output: Output, rawBuffer: ByteBuffer): Unit = {
    // Copy the buffer in order to not modify it
    val buffer = rawBuffer.clone.asInstanceOf[ByteBuffer]
    val length = buffer.remaining

    val (arr, off, len) = if (buffer.hasArray) {
      (buffer.array, buffer.position, length)
    } else {
      val bytes = new Array[Byte](length)
      buffer.get(bytes, 0, length)
      (bytes, 0, length)
    }

    ByteArrayUtils.write(output, arr, off, len)
  }

  def readRemaining(input: Input): ByteBuffer = {
    val bytes = ByteArrayUtils.read(input)
    val result = ByteBuffer.wrap(bytes)
    result
  }
}
*/