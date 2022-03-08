import cats.effect.IO
import fs2.Stream
import munit.CatsEffectSuite
import org.apache.hadoop.io.{BooleanWritable, IntWritable}
import sequence_fs2.SequenceFileFs2

class SequenceFileSuite extends CatsEffectSuite {

  test("Write and read a simple SequenceFile") {
    val stream = Stream.range(0, 50).evalMap(i => IO(new IntWritable(i), new BooleanWritable(i % 2 == 0)))

    val write = stream.through(SequenceFileFs2.write("./data/test_1")).compile.drain

    val read = SequenceFileFs2.read[IO, IntWritable, BooleanWritable]("./data/test_1")

    write >> read.filter(_._2.get()).compile.count.map(c => assertEquals(c, 25L))
  }
}
