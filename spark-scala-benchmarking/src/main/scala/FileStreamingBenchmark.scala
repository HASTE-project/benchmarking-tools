/* SimpleApp.scala */

import java.time.Instant

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.Random


object FileStreamingBenchmark {

  // NOTE: remember to update the matching const in the python code.
  val BATCH_INTERVAL = 5

  //val r = Random.javaRandomToRandom(new java.util.Random())

  def pause(pause_secs: Float) = {

    val start = Instant.now.getEpochSecond
    var dummyResult: Float = 0
    while (start + pause_secs > Instant.now.getEpochSecond) {
      // Keep the CPU busy
      for (i <- 1 to 1000) {
        dummyResult = i
      }
    }
    println(Instant.now.getEpochSecond - (start + pause_secs))
    dummyResult
  }

  def process_line(line: String): Float = {
    val pause_secs = line.substring(1, 7).toFloat / 1000
    var dummyResult: Float = pause(pause_secs)
    dummyResult
  }

  def includePath(path: Path): Boolean = {


    // Exclude old files - so that the last modified time check is skipped, so we don't crash
    // if the file is deleted.
    // See: https://stackoverflow.com/questions/50058451/when-is-it-safe-to-delete-files-when-using-file-based-streaming/50062050#50062050
    //println("filter path")
    val name = path.getName

    //val localhostname = java.net.InetAddress.getLocalHost.getHostName
    // println(localhostname) 'ben-spark-master' -- is this the driver app ?

    // This is what the default filter does.
    if (name.startsWith(".")) return false

    val filename_parts = name.split("_")
    val unix_timestamp = filename_parts(0).toInt
    //println(filename_parts(0))

    // This means if file listing overruns significantly, we start to drop files.
    // This is OK - we throttle back in this case anyway
    Instant.now.getEpochSecond < unix_timestamp + BATCH_INTERVAL * 2
  }

  def main(args: Array[String]) {

    runSparkApp

    //pause(0.0001f)
  }

  private def runSparkApp = {
    val dir = "/mnt/nfs/ben-stream-src-3/bench2/"

    val sparkSession = SparkSession.builder
      .master("spark://ben-spark-master:7077")
      .appName("File Streaming")
      .config("spark.streaming.unpersist", "True") // Not sure this matters, we arn't caching anyway
      .config("spark.streaming.fileStream.minRememberDuration", "60s") // big so file info gets cached.
      .getOrCreate()

    // Leaving this as default 60

    sparkSession.sparkContext.setLogLevel("INFO") // Need info to read "Finding new files took ...."

    val logger = Logger.getLogger("file-streaming-app")

    logger.info("test log message")

    if (true) {
      val ssc = new StreamingContext(sparkSession.sparkContext, Seconds(BATCH_INTERVAL))

      ssc.fileStream[LongWritable, Text, TextInputFormat](dir,
        filter = includePath(_),
        newFilesOnly = true)
        .map(_._2.toString)
        .map(line => process_line(line))
        .foreachRDD(rdd => println("processed: " + rdd.count()))
      ssc.start()
      ssc.awaitTermination()
    }
  }
}