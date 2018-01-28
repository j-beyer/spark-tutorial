package de.hpi.ind

import java.io.File

import com.beust.jcommander.{JCommander, Parameter, ParameterException}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._


object IndApp extends App {
  val indApp = new IndApp()
  val commander = JCommander.newBuilder().addObject(indApp).build()

  try {
    commander.parse(args: _*)
  } catch {
    case e: ParameterException =>
      println(s"Could not parse args!\n${e.getMessage}")
      System.exit(1)
    case e: Throwable =>
      println(s"Unknown exception!\n${e.getMessage}")
      System.exit(1)
  }

  indApp.run()
}

class IndApp {

  @Parameter(names = Array("--path", "-p"), required = false)
  var path: String = "./TPCH"

  @Parameter(names = Array("--cores", "-c"), required = false)
  var numCores: Int = 4

  // as taken from the tutorial
  val numPartitions: Int = 5


  def run(): Unit = {

    // Turn off logging
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    // Create a SparkSession to work with Spark
    val sparkBuilder = SparkSession
      .builder()
      .appName("IndFinder")
      // local, with 4 worker cores
      .master(s"local[$numCores]")
    val spark = sparkBuilder.getOrCreate()

    spark.sparkContext.setLogLevel("OFF")

    // Set the default number of shuffle partitions to 5 (default is 200, which is too high for local deployment)
    spark.conf.set("spark.sql.shuffle.partitions", s"$numPartitions")

    // Importing implicit encoders for standard library classes and tuples that are used as Dataset types
    import spark.implicits._


    val files = getListOfFiles(path)

    // read all files into dataframes
    val dataFrames = files.map(file =>
      spark.read
        .option("inferSchema", "false")
        .option("header", "true")
        .option("sep", ";")
        .csv(s"$file")
    )

    // extract cells from each dataframe, and union the resulting cells
    val cells = dataFrames
      .map(dataFrame => {
        val columnNames = dataFrame.columns
        val rowCells = dataFrame
          .map(row => row.toSeq.asInstanceOf[Seq[String]])
          .flatMap(row => row.zip(columnNames))
          .map(pair => pair._1 -> Seq(pair._2))
        rowCells
      })
      .reduce(_ union _)

    // generate attribute sets
    val attributeSets = cells.rdd
      .reduceByKey(_ ++ _ distinct)
      .map(_._2)

    // generate inclusion lists
    val inclusionLists = attributeSets
      .flatMap(attributeSet => {
        val set = attributeSet.toSet
        set.map(el => (el, (set - el).toSeq)).toSeq
      })

    // aggregate, i.e., intersect inclusion list values
    val aggregated = inclusionLists
      .reduceByKey(_ intersect _)
      .filter(_._2.nonEmpty)
      .sortByKey(ascending = true, 1)

    // print output
    aggregated.foreach(pair => println(s"${pair._1} < ${pair._2.mkString(", ")}"))

  }

  private def getListOfFiles(dir: String): List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }
}
