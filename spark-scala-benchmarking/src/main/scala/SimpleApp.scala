/* SimpleApp.scala */

import java.time.Instant

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.SparkContext
import org.apache.spark.rdd.UnionRDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.Random


object SimpleApp {

  //  def fileNameFilter(path: Path): Boolean = {
  //    if (path.getName().contains("COPYING")) {
  //      //logger.info("*** ignoring incomplete file: " + path.getName())
  //      return false
  //    } else {
  //      return true
  //    }
  //  }
  //
  //  def deleteFile(sc: SparkContext, fileName: String): Unit = {
  //    val filePath = new Path(fileName)
  //    val fs = FileSystem.get(new Configuration())
  //    if (fs.isDirectory(filePath)) {
  //      fs.listStatus(filePath).foreach((status) => {
  //        fs.delete(status.getPath(), true)
  //      })
  //    } else {
  //      fs.delete(filePath, true)
  //    }
  //  }

  val BATCH_INTERVAL = 5

  def process_line(line: String): Float = {
    val pause_secs = line.substring(1, 7).toFloat / 1000

    val start = Instant.now.getEpochSecond
    val r = Random.javaRandomToRandom(new java.util.Random())

    var result: Float = 0

    while (start + pause_secs > Instant.now.getEpochSecond) {
      for (i <- 1 to 1000) {
        val a = r.nextFloat()
        val b = r.nextFloat()
        result = a * b
      }
    }
    result
  }

  def includePath(path: Path): Boolean = {
    // Exclude old files - so that the last modified time check is skipped, so we don't crash
    // if the file is deleted.
    // See: https://stackoverflow.com/questions/50058451/when-is-it-safe-to-delete-files-when-using-file-based-streaming/50062050#50062050
    //println("filter path")
    val name = path.getName

    val localhostname = java.net.InetAddress.getLocalHost.getHostName
    // println(localhostname) 'ben-spark-master' -- is this the driver app ?

    if (name.startsWith(".")) return false

    val filename_parts = name.split("_")
    val unix_timestamp = filename_parts(0).toInt
    //println(filename_parts(0))

    // This means if file listing overruns, we start to drop files.
    // This is OK
    Instant.now.getEpochSecond < unix_timestamp + BATCH_INTERVAL * 2
  }

  def main(args: Array[String]) {

    val dir = "/mnt/nfs/ben-stream-src-3/bench2/"

    val sparkSession = SparkSession.builder
      .master("spark://ben-spark-master:7077")
      .appName("File Streaming")
      .config("spark.streaming.unpersist", "True")
      .config("spark.streaming.fileStream.minRememberDuration", "60s") // big so file info gets cached.
      .getOrCreate()

    // Leaving this as default 60


    sparkSession.sparkContext.setLogLevel("WARN")

    if (true) {
      val ssc = new StreamingContext(sparkSession.sparkContext, Seconds(BATCH_INTERVAL))

      ssc.fileStream[LongWritable, Text, TextInputFormat](dir,
        filter = includePath(_), // TODO: check last modified time here.
        newFilesOnly = true)
        .map(_._2.toString)
        .map(line => process_line(line))
        .foreachRDD(rdd => println(rdd.count()))

      ssc.start()
      ssc.awaitTermination()
    }


  }
}