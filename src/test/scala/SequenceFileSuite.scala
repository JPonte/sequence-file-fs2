import cats.effect.IO
import fs2.Stream
import munit.CatsEffectSuite
import org.apache.hadoop.io.SequenceFile.CompressionType
import sequence_fs2.SequenceFileFs2

class SequenceFileSuite extends CatsEffectSuite {

  test("Write and read a simple SequenceFile") {
    val stream = Stream.range(0, 50).evalMap(i => IO(i -> (i % 2 == 0)))

    val write = stream.through(SequenceFileFs2.write("./data/test_1")).compile.drain

    val read = SequenceFileFs2.read[IO, Int, Boolean]("./data/test_1")

    write >> read.filter(_._2).compile.count.map(c => assertEquals(c, 25L))
  }

  test("Write and read a SequenceFile with an array") {
    val recordCount = 500
    val arraySize = 200

    val stream = Stream.range(0, recordCount).evalMap(i => IO(i -> (0 until arraySize).toArray))

    val write = stream.through(SequenceFileFs2.write("./data/test_1", CompressionType.BLOCK)).compile.drain

    val read = SequenceFileFs2.read[IO, Int, Array[Int]]("./data/test_1")

    write >> read.filter(_._2.length == arraySize).compile.count.map(c => assertEquals(c, recordCount.toLong))
  }

  test("Write and read a SequenceFile with a map") {
    val recordCount = 500
    val mapSize = 200

    val stream = Stream.range(0, recordCount).evalMap(i => IO(i -> (0 until mapSize).map(k => k -> k.toString).toMap))

    val write = stream.through(SequenceFileFs2.write("./data/test_1", CompressionType.BLOCK)).compile.drain

    val read = SequenceFileFs2.read[IO, Int, Map[Int, String]]("./data/test_1")

    write >> read.filter(_._2.keys.size == mapSize).compile.count.map(c => assertEquals(c, recordCount.toLong))
  }
}
