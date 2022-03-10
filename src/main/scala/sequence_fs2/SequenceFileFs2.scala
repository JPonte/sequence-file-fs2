package sequence_fs2

import cats.effect.Sync
import fs2.{Pipe, Stream}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.SequenceFile.{CompressionType, Reader, Writer}
import org.apache.hadoop.io._

import java.nio.ByteBuffer
import scala.collection.JavaConverters.asScalaSetConverter
import scala.language.higherKinds
import scala.reflect.ClassTag

object SequenceFileFs2 {

  trait WritableProvider[T] {
    def getEmptyWritable: Writable

    def getWritable(t: T): Writable

    def getValue(w: Writable): T
  }

  implicit val nullWritableProvider: WritableProvider[Unit] = new WritableProvider[Unit] {
    override def getEmptyWritable: Writable = NullWritable.get()

    override def getWritable(t: Unit): Writable = getEmptyWritable

    override def getValue(w: Writable): Unit = ()
  }

  implicit val bytesWritableProvider: WritableProvider[ByteBuffer] =
    new WritableProvider[ByteBuffer] {
      override def getEmptyWritable: Writable = new BytesWritable()

      override def getWritable(t: ByteBuffer): Writable = new BytesWritable(t.array())

      override def getValue(w: Writable): ByteBuffer = {
        val bw = w.asInstanceOf[BytesWritable]
        ByteBuffer.wrap(bw.getBytes, 0, bw.getLength)
      }
    }

  implicit val intWritableProvider: WritableProvider[Int] = new WritableProvider[Int] {
    override def getEmptyWritable: Writable = new IntWritable()

    override def getWritable(t: Int): Writable = new IntWritable(t)

    override def getValue(w: Writable): Int = w.asInstanceOf[IntWritable].get()
  }

  implicit val longWritableProvider: WritableProvider[Long] = new WritableProvider[Long] {
    override def getEmptyWritable: Writable = new LongWritable()

    override def getWritable(t: Long): Writable = new LongWritable(t)

    override def getValue(w: Writable): Long = w.asInstanceOf[LongWritable].get()
  }

  implicit val floatWritableProvider: WritableProvider[Float] = new WritableProvider[Float] {
    override def getEmptyWritable: Writable = new FloatWritable()

    override def getWritable(t: Float): Writable = new FloatWritable(t)

    override def getValue(w: Writable): Float = w.asInstanceOf[FloatWritable].get()
  }

  implicit val doubleWritableProvider: WritableProvider[Double] = new WritableProvider[Double] {
    override def getEmptyWritable: Writable = new DoubleWritable()

    override def getWritable(t: Double): Writable = new DoubleWritable(t)

    override def getValue(w: Writable): Double = w.asInstanceOf[DoubleWritable].get()
  }

  implicit val boolWritableProvider: WritableProvider[Boolean] = new WritableProvider[Boolean] {
    override def getEmptyWritable: Writable = new BooleanWritable()

    override def getWritable(t: Boolean): Writable = new BooleanWritable(t)

    override def getValue(w: Writable): Boolean = w.asInstanceOf[BooleanWritable].get()
  }

  implicit val stringWritableProvider: WritableProvider[String] = new WritableProvider[String] {
    override def getEmptyWritable: Writable = new Text()

    override def getWritable(t: String): Writable = new Text(t)

    override def getValue(w: Writable): String = w.asInstanceOf[Text].toString
  }

  implicit def arrayWritableProvider[T: WritableProvider: ClassTag]: WritableProvider[Array[T]] =
    new WritableProvider[Array[T]] {

      override def getEmptyWritable: Writable = {
        val innerWritable = implicitly[WritableProvider[T]].getEmptyWritable
        new ArrayWritable(innerWritable.getClass)
      }

      override def getWritable(t: Array[T]): Writable = {
        val innerWritableProvider = implicitly[WritableProvider[T]]
        new ArrayWritable(
          innerWritableProvider.getEmptyWritable.getClass,
          t.map(innerWritableProvider.getWritable)
        )
      }

      override def getValue(w: Writable): Array[T] = {
        val innerWritable = implicitly[WritableProvider[T]]
        w.asInstanceOf[ArrayWritable].get().map(innerWritable.getValue)
      }
    }

  implicit def mapWritableProvider[K: WritableProvider: ClassTag, V: WritableProvider: ClassTag]
      : WritableProvider[Map[K, V]] = new WritableProvider[Map[K, V]] {
    override def getEmptyWritable: Writable = new MapWritable()

    override def getWritable(t: Map[K, V]): Writable = {
      val mapWritable = new MapWritable()
      val kProvider = implicitly[WritableProvider[K]]
      val vProvider = implicitly[WritableProvider[V]]
      t.foreach { case (k, v) =>
        mapWritable.put(kProvider.getWritable(k), vProvider.getWritable(v))
      }
      mapWritable
    }

    override def getValue(w: Writable): Map[K, V] = {
      val kProvider = implicitly[WritableProvider[K]]
      val vProvider = implicitly[WritableProvider[V]]
      val mapWritable = w.asInstanceOf[MapWritable]
      mapWritable
        .keySet()
        .asScala
        .map { k =>
          kProvider.getValue(k) -> vProvider.getValue(mapWritable.get(k))
        }
        .toMap
    }
  }

  /** Creates an [[fs2.Stream]] that reads from the specified Sequence File. The key and values of
    * the records need to have an implicit [[WritableProvider]] in scope, that can wrap its values
    * into a hadoop Writable.
    *
    * @param filename
    *   the file path of the Sequence File
    * @param bufferSize
    *   the buffer size of the internal reader
    * @param config
    *   Hadoop Configuration for the internal reader
    * @tparam F
    *   effect type
    * @tparam K
    *   type of the record key
    * @tparam V
    *   type of the record value
    * @return
    *   [[fs2.Stream]] of records of type (K, V)
    */
  def read[F[_]: Sync, K: WritableProvider, V: WritableProvider](
      filename: String,
      bufferSize: Option[Int] = None,
      config: Configuration = new Configuration()
  ): Stream[F, (K, V)] = {
    val opts =
      List(Option(Reader.file(new Path(filename))), bufferSize.map(Reader.bufferSize)).flatten
    val acquireReader = Sync[F].blocking(new SequenceFile.Reader(config, opts: _*))
    Stream.bracket(acquireReader)(reader => Sync[F].blocking(reader.close())).flatMap { reader =>
      Stream.unfoldEval(reader) { _ =>
        Sync[F].blocking {
          val kWritableProvider = implicitly[WritableProvider[K]]
          val vWritableProvider = implicitly[WritableProvider[V]]
          val kWritable = kWritableProvider.getEmptyWritable
          val vWritable = vWritableProvider.getEmptyWritable
          if (reader.next(kWritable, vWritable)) {
            Option(
              (
                (kWritableProvider.getValue(kWritable), vWritableProvider.getValue(vWritable)),
                reader
              )
            )
          } else {
            None
          }
        }
      }
    }
  }

  /** Creates an [[fs2.Pipe]] that writes data to a single Sequence File. The key and values of the
    * records need to have an implicit [[WritableProvider]] in scope, that can wrap its values into
    * a hadoop Writable.
    *
    * @param filename
    *   the file path of the Sequence File
    * @param compressionType
    *   one of NONE, BLOCK, RECORD compression types
    * @param config
    *   Hadoop Configuration for the internal writer
    * @tparam F
    *   effect type
    * @tparam K
    *   type of the record key
    * @tparam V
    *   type of the record value
    * @return
    *   writer [[fs2.Pipe]]
    */
  def write[F[_]: Sync, K: WritableProvider, V: WritableProvider](
      filename: String,
      compressionType: CompressionType = CompressionType.NONE,
      config: Configuration = new Configuration()
  ): Pipe[F, (K, V), (K, V)] = { in =>
    {
      val acquireWriter = Sync[F].blocking(
        SequenceFile.createWriter(
          config,
          Writer.file(new Path(filename)),
          Writer.keyClass(implicitly[WritableProvider[K]].getEmptyWritable.getClass),
          Writer.valueClass(implicitly[WritableProvider[V]].getEmptyWritable.getClass),
          Writer.compression(compressionType)
        )
      )

      Stream.bracket(acquireWriter)(writer => Sync[F].blocking(writer.close())).flatMap { writer =>
        val kWritableProvider = implicitly[WritableProvider[K]]
        val vWritableProvider = implicitly[WritableProvider[V]]
        in.evalMap { case (k, v) =>
          Sync[F].blocking {
            writer.append(kWritableProvider.getWritable(k), vWritableProvider.getWritable(v))
            (k, v)
          }
        }
      }
    }
  }
}
