package sequence_fs2

import cats.effect.Sync
import fs2.{Pipe, Stream}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.SequenceFile.{CompressionType, Reader, Writer}
import org.apache.hadoop.io.{ArrayWritable, BooleanWritable, BytesWritable, DoubleWritable, FloatWritable, IntWritable, LongWritable, MapWritable, NullWritable, SequenceFile, Writable}

import scala.reflect.{ClassTag, classTag}


object SequenceFileFs2 {
  trait WritableProvider[W <: Writable] {
    def get: W
  }

  implicit val nullWritableProvider: WritableProvider[NullWritable] = new WritableProvider[NullWritable] {
    override def get: NullWritable = NullWritable.get()
  }

  implicit val bytesWritableProvider: WritableProvider[BytesWritable] = new WritableProvider[BytesWritable] {
    override def get: BytesWritable = new BytesWritable()
  }

  implicit val intWritableProvider: WritableProvider[IntWritable] = new WritableProvider[IntWritable] {
    override def get: IntWritable = new IntWritable()
  }

  implicit val longWritableProvider: WritableProvider[LongWritable] = new WritableProvider[LongWritable] {
    override def get: LongWritable = new LongWritable()
  }

  implicit val floatWritableProvider: WritableProvider[FloatWritable] = new WritableProvider[FloatWritable] {
    override def get: FloatWritable = new FloatWritable()
  }

  implicit val doubleWritableProvider: WritableProvider[DoubleWritable] = new WritableProvider[DoubleWritable] {
    override def get: DoubleWritable = new DoubleWritable()
  }

  implicit val boolWritableProvider: WritableProvider[BooleanWritable] = new WritableProvider[BooleanWritable] {
    override def get: BooleanWritable = new BooleanWritable()
  }

  implicit def arrayWritableProvider[T <: Writable : ClassTag]: WritableProvider[ArrayWritable] = new WritableProvider[ArrayWritable] {
    override def get: ArrayWritable = new ArrayWritable(classTag[T].runtimeClass.asInstanceOf[Class[Writable]])
  }

  implicit def mapWritableProvider: WritableProvider[MapWritable] = new WritableProvider[MapWritable] {
    override def get: MapWritable = new MapWritable()
  }

  def read[F[_] : Sync, K <: Writable : WritableProvider, V <: Writable : WritableProvider]
  (filename: String,
   start: Option[Long] = None,
   length: Option[Long] = None,
   bufferSize: Option[Int] = None,
   config: Configuration = new Configuration()): Stream[F, (K, V)] = {
    val opts = List(Option(Reader.file(new Path(filename))), start.map(Reader.start), length.map(Reader.length), bufferSize.map(Reader.bufferSize)).flatten
    val acquireReader = Sync[F].blocking(new SequenceFile.Reader(config, opts: _*))
    Stream.bracket(acquireReader)(reader => Sync[F].blocking(reader.close())).flatMap { reader =>
      Stream.unfoldEval(reader) { _ =>
        Sync[F].blocking {
          val kWritable = implicitly[WritableProvider[K]].get
          val vWritable = implicitly[WritableProvider[V]].get
          if (reader.next(kWritable, vWritable)) {
            Option(((kWritable, vWritable), reader))
          } else {
            None
          }
        }
      }
    }
  }

  def write[F[_] : Sync, K <: Writable : ClassTag, V <: Writable : ClassTag](filename: String, compressionType: CompressionType = CompressionType.NONE, config: Configuration = new Configuration()): Pipe[F, (K, V), (K, V)] = {
    in => {
      val acquireWriter = Sync[F].blocking(SequenceFile.createWriter(config, Writer.file(new Path(filename)),
        Writer.keyClass(classTag[K].runtimeClass.asInstanceOf[Class[Writable]]),
        Writer.valueClass(classTag[V].runtimeClass.asInstanceOf[Class[Writable]]),
        Writer.compression(compressionType)))

      Stream.bracket(acquireWriter)(writer => Sync[F].blocking(writer.close())).flatMap { writer =>
        in.evalMap { case (k, v) =>
          Sync[F].blocking {
            writer.append(k, v)
            (k, v)
          }
        }
      }
    }
  }
}
