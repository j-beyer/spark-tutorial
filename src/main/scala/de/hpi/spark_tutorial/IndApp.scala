package de.hpi.spark_tutorial

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
    spark.conf.set("spark.sql.shuffle.partitions", s"$numPartitions") //
    spark.conf.set("spark.sql.crossJoin.enabled", "true")

    // Importing implicit encoders for standard library classes and tuples that are used as Dataset types
    import spark.implicits._

    //
    //    val stringTypedDS = untypedDF.map(r => r.get(0).toString) // DF to DS via map
    //    val integerTypedDS = untypedDF.as[Int] // DF to DS via as() function that cast columns to a concrete types
    //
    //    // Grouping and aggregation for Datasets
    //    val topEarners = employees
    //      .groupByKey { case (name, age, salary, company) => company }
    //      .mapGroups { case (key, iterator) =>
    //          val topEarner = iterator.toList.maxBy(t => t._3) // could be problematic: Why?
    //          (key, topEarner._1, topEarner._3)
    //      }
    //      .sort(desc("_3"))
    //    topEarners.collect().foreach(t => println(t._1 + "'s top earner is " + t._2 + " with salary " + t._3))
    //
    //    employees.printSchema() // print schema of dataset/dataframe
    //    topEarners.explain() // print Spark's physical query plan for this dataset/dataframe
    //    topEarners.show() // print the content of this dataset/dataframe
    //
    //
    //
    //    persons
    //      .map(_.name + " says hello")
    //      .collect()
    //      .foreach(println(_))
    //
    //
    //    // Solution: broadcast variable
    //    val bcNames = spark.sparkContext.broadcast(names)
    //    val bcFiltered1 = employees.filter(e => bcNames.value.contains(e._1)) // a copy of names is shipped to each executor
    //    val bcFiltered2 = employees.filter(e => !bcNames.value.contains(e._1)) // a copy of names is already present
    //    val bcFiltered3 = employees.filter(e => bcNames.value(1).equals(e._1)) // a copy of names is already present
    //    List(bcFiltered1, bcFiltered2, bcFiltered3).foreach(_.show(1))
    //    bcNames.destroy() // finally, destroy the broadcast variable to release it from memory in each executor
    //
    //    val region = spark.read
    //      .option("inferSchema", "true")
    //      .option("header", "true")
    //      .option("sep", ";")
    //      .csv(s"$path/tpch_region.csv")
    //
    //    val columnNames = region.columns
    //
    //    val regionCells = region.rdd.map(row => row.toSeq.zip(columnNames)).collect
    //    regionCells.foreach(println(_))

    val files = getListOfFiles(path)

    val dataFrames = files.map(file =>
      spark.read
        .option("inferSchema", "true")
        .option("header", "true")
        .option("sep", ";")
        .csv(s"$file")
    )

    val cells = dataFrames
      .flatMap(dataFrame => {
        println(s"zipping for ... ${dataFrame.columns(0)}")
        val columnNames = dataFrame.columns
        val rowCells = dataFrame
          .map(row => row.toSeq.asInstanceOf[Seq[String]])
          .flatMap(row => row.zip(columnNames))
          .collect
        rowCells
      })
      .toDS
      .map(pair => pair._1->Seq(pair._2))


    // preaggregation, concat column title for same value
    val preAggregatedCells = cells.groupByKey(value => value._1)
      .reduceGroups((cell1, cell2) => (cell1._1, cell1._2 ++ cell2._2))
      .map(_._2)
      .map(pair => (pair._1, pair._2.distinct))

    // redistribute by key
    val repartitionedCells = preAggregatedCells.repartition(numPartitions)

    repartitionedCells.show

    // generate attribute sets
    val attributeSets = repartitionedCells
      .groupByKey(value => value._1)
      .reduceGroups((cell1, cell2) => (cell1._1, cell1._2 ++ cell2._2))
      .map(_._2)
      .map(pair => pair._2.distinct)
      .distinct()

    attributeSets.show

    // generate inclusion lists
    val inclusionLists = attributeSets
      .flatMap(attributeSet => {
        val set = attributeSet.toSet
        set.map(el => (el, (set - el).toSeq)).toSeq
      })

    // redistribute by key
    val repartitionedInclusionLists = inclusionLists.repartition(numPartitions)

    repartitionedInclusionLists.show

    // aggregate, i.e., intersect inclusion list values
    val aggregated = repartitionedInclusionLists
      .groupByKey(value => value._1)
      .reduceGroups((cell1, cell2) => (cell1._1, cell1._2.toSet.intersect(cell2._2.toSet).toSeq))
      .map(_._2)

      // remove empty sets
      .filter(_._2.nonEmpty)

    val inds = aggregated
      .map(pair => pair._1 + " < " + pair._2.mkString(", "))
      .collect

    inds.foreach(println(_))

  }

  private def zipRowWithColumnNames(dataFrame: DataFrame): Array[(String, String)] = {
    println("zipRowWithColumnNames called")
    val columnNames = dataFrame.columns
    val cells = dataFrame
      .map(row => row.toSeq.asInstanceOf[Seq[String]])
      .flatMap(row => row.zip(columnNames))
    cells
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
