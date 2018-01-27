package de.hpi.spark_tutorial

import com.beust.jcommander.{JCommander, Parameter, ParameterException}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


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

  main.run()
}

class IndApp {

  @Parameter(names = Array("--path", "-p"), required = false)
  var path: String = "./TPCH"

  @Parameter(names = Array("--cores", "-c"), required = false)
  var numCores: Int = 4


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

    // Set the default number of shuffle partitions to 5 (default is 200, which is too high for local deployment)
    spark.conf.set("spark.sql.shuffle.partitions", "5") //
    spark.conf.set("spark.sql.crossJoin.enabled", "true")
    
    // Importing implicit encoders for standard library classes and tuples that are used as Dataset types
    import spark.implicits._

//
//    // DataFrame and Dataset
//    val untypedDF = numbers.toDF() // DS to DF
//    val stringTypedDS = untypedDF.map(r => r.get(0).toString) // DF to DS via map
//    val integerTypedDS = untypedDF.as[Int] // DF to DS via as() function that cast columns to a concrete types
//    List(untypedDF, stringTypedDS, integerTypedDS).foreach(result => println(result.head.getClass))
//    List(untypedDF, stringTypedDS, integerTypedDS).foreach(result => println(result.head))
//
//    println("--------------------------------------------------------------------------------------------------------------")
//
//    // SQL on DataFrames
//    employees.createOrReplaceTempView("employee") // make this dataframe visible as a table
//    val sqlResult = spark.sql("SELECT * FROM employee WHERE Age > 95") // perform an sql query on the table
//
//    import org.apache.spark.sql.functions._
//
//    sqlResult // DF
//      .as[(String, Int, Double, String)] // DS
//      .sort(desc("Salary")) // desc() is a standard function from the spark.sql.functions package
//      .head(10)
//      .foreach(println(_))
//
//    println("--------------------------------------------------------------------------------------------------------------")
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
//    println("--------------------------------------------------------------------------------------------------------------")
//
//    //------------------------------------------------------------------------------------------------------------------
//    // Analyzing Datasets and DataFrames
//    //------------------------------------------------------------------------------------------------------------------
//
//    employees.printSchema() // print schema of dataset/dataframe
//    topEarners.explain() // print Spark's physical query plan for this dataset/dataframe
//    topEarners.show() // print the content of this dataset/dataframe
//
//    //------------------------------------------------------------------------------------------------------------------
//    // Dates and Null Values
//    //------------------------------------------------------------------------------------------------------------------
//
//    println("--------------------------------------------------------------------------------------------------------------")
//
//    import org.apache.spark.sql.functions.{current_date, current_timestamp, lit, col, date_add}
//
//    // Create a data frame with 5 records holding a date, a timestamp, and a null column
//    val dateDF = spark.range(5)
//      .withColumn("date_today", current_date())
//      .withColumn("stamp_now", current_timestamp())
//      .withColumn("nulls", lit(null).cast("string"))
//    dateDF.show()
//
//    println("--------------------------------------------------------------------------------------------------------------")
//
//    // Fill nulls and move date to next week
//    val filledNulls = dateDF
//      .na.fill("no_null_value", Seq("today", "now", "nulls")) // fill nulls
//      .select(
//        col("id"),
//        date_add(col("date_today"), 7).as("date_next_week"), // date next week
//        col("nulls").as("no_nulls"),
//        col("nulls").isNull.as("is_null")) // is-null-check
//    filledNulls.show()
//
//    println("--------------------------------------------------------------------------------------------------------------")
//
//    //------------------------------------------------------------------------------------------------------------------
//    // Custom types
//    //------------------------------------------------------------------------------------------------------------------
//
//    // (see definition above) A Scala case class; works out of the box as Dataset type using Spark's implicit encoders
////   case class Person(name:String, surname:String, age:Int)
//
//    val persons = spark.createDataset(List(
//      Person("Barack", "Obama", 40),
//      Person("George", "R.R. Martin", 65),
//      Person("Elon", "Musk", 34)))
//
//    persons
//      .map(_.name + " says hello")
//      .collect()
//      .foreach(println(_))
//
//    println("------------------------------------------")
//
//    // (see definition above) A non-case class; requires an encoder to work as Dataset type
// //   class Pet(var name:String, var age:Int) {
// //     override def toString = s"Pet(name=$name, age=$age)"
// //   }
//
//    implicit def PetEncoder: Encoder[Pet] = org.apache.spark.sql.Encoders.kryo[Pet]
//
//    val pets = spark.createDataset(List(
//      new Pet("Garfield", 5),
//      new Pet("Paddington", 2)))
//
//    pets
//      .map(_ + " is cute") // our Pet encoder gets passed to method implicitly
//      .collect()
//      .foreach(println(_))
//
//    println("--------------------------------------------------------------------------------------------------------------")
//
//    //------------------------------------------------------------------------------------------------------------------
//    // Shared Variables across Executors
//    //------------------------------------------------------------------------------------------------------------------
//
//    // The problem: shipping large variables multiple times to executors is expensive
//    val names = List("Berget", "Bianka", "Cally")
//    val filtered1 = employees.filter(e => names.contains(e._1)) // a copy of names is shipped to each executor
//    val filtered2 = employees.filter(e => !names.contains(e._1)) // a copy of names is shipped to each executor again!
//    val filtered3 = employees.filter(e => names(1).equals(e._1)) // a copy of names is shipped to each executor again!
//    List(filtered1, filtered2, filtered3).foreach(_.show(1))
//
//    println("--------------------------------------------------------------------------------------------------------------")
//
//    // Solution: broadcast variable
//    val bcNames = spark.sparkContext.broadcast(names)
//    val bcFiltered1 = employees.filter(e => bcNames.value.contains(e._1)) // a copy of names is shipped to each executor
//    val bcFiltered2 = employees.filter(e => !bcNames.value.contains(e._1)) // a copy of names is already present
//    val bcFiltered3 = employees.filter(e => bcNames.value(1).equals(e._1)) // a copy of names is already present
//    List(bcFiltered1, bcFiltered2, bcFiltered3).foreach(_.show(1))
//    bcNames.destroy() // finally, destroy the broadcast variable to release it from memory in each executor
//
//    println("--------------------------------------------------------------------------------------------------------------")
//
//    // Accumulators:
//    // - shared variables that distributed executors can add to
//    // - enable side-effects in pipeline computations (e.g. for debugging)
//    // - only usable in actions, not in transformations (transformations should not have side effects as they may be re-run!)
//    // - should be used with care: http://imranrashid.com/posts/Spark-Accumulators/
//    val appleAccumulator = spark.sparkContext.longAccumulator("Apple Accumulator")
//    val microsoftAccumulator = spark.sparkContext.longAccumulator("Microsoft Accumulator")
//    employees.foreach(
//      e => if (e._4 == "Apple") appleAccumulator.add(1)
//      else if (e._4 == "Microsoft") microsoftAccumulator.add(1)
//      /* ... */) // accumulators are useful only if the action also does something else is; otherwise use filter and count!
//    println("There are " + appleAccumulator.value + " employees at Apple")
//    println("There are " + microsoftAccumulator.value + " employees at Microsoft")
//
//    println("--------------------------------------------------------------------------------------------------------------")
//
//    //------------------------------------------------------------------------------------------------------------------
//    // Machine Learning
//    // https://spark.apache.org/docs/2.2.0/ml-classification-regression.html#decision-tree-classifier
//    //------------------------------------------------------------------------------------------------------------------
//
//    val data = spark
//      .read
//      .format("libsvm")
//      .load("data/sample_libsvm_data.txt")
//
//    data.printSchema() // Spark reads the libsvm data into a dataframe with two columns: label and features
//    data.show(10)
//
//    println("--------------------------------------------------------------------------------------------------------------")
//
//    // Concepts in Spark's machine learning module:
//    // - Transformer: An algorithm (function) that transforms one DataFrame into another DataFrame.
//    //                I.e. an ML model that transforms a DataFrame of features into a DataFrame of predictions.
//    // - Estimator:   An algorithm (function) that trains a Transformer on a DataFrame.
//    //                I.e. a learning algorithm (e.g. DecisionTree) that trains on a DataFrame and produces a model.
//    // - Pipeline:    A directed acyclic graph (DAG) chaining multiple Transformers and Estimators together.
//    //                I.e. a ML workflow specification.
//
//    // Automatically identify categorical features, and index them.
//    // The Vectors only contain numerical values, so we need to flag which values are categorical
//    // A VectorIndexer is a transformer that, in this case, adds a column.
//    val featureIndexer = new VectorIndexer()
//      .setInputCol("features")
//      .setOutputCol("indexedFeatures")
//      .setMaxCategories(4) // features with > 4 distinct values are treated as continuous.
//      .fit(data) // fit the indexer to the data (= "parameterize" and not "train")
//
//    // Split the data into training and test sets (30% held out for testing)
//    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))
//
//    println("--------------------------------------------------------------------------------------------------------------")
//
//    // Create a Decision Tree Classifier
//    val decisionTree = new DecisionTreeClassifier()
//      .setLabelCol("label")
//      .setFeaturesCol("indexedFeatures")
//
//    // Chain indexer and tree in a Pipeline
//    val pipeline = new Pipeline()
//      .setStages(Array(featureIndexer, decisionTree))
//
//    println("--------------------------------------------------------------------------------------------------------------")
//
//    // Run the entire pipeline:
//    // 1. The featureIndexer adds the column "indexedFeatures"
//    // 2. The decisionTree trains the decision tree model
//    val model = pipeline.fit(trainingData) // produces a model, which is a new transformer
//
//    // Print the learned decision tree model
//    val treeModel = model
//      .stages(1)
//      .asInstanceOf[DecisionTreeClassificationModel]
//    println("Learned classification tree model:\n" + treeModel.toDebugString)
//
//    // Make predictions in an additional column in the output dataframe with default name "prediction"
//    val predictions = model.transform(testData)
//
//    println("--------------------------------------------------------------------------------------------------------------")
//
//    // Select example rows to display
//    predictions
//      .select("prediction", "label", "features", "indexedFeatures")
//      .show(10)
//
//    // Evaluate the error for the predictions on the test dataset using its predicted and true labels
//    val evaluator = new MulticlassClassificationEvaluator() // a convenience class for calculating a model's accuracy
//      .setLabelCol("label")
//      .setPredictionCol("prediction")
//      .setMetricName("accuracy")
//    val accuracy = evaluator.evaluate(predictions)
//    println("Test error = " + (1.0 - accuracy))
//
//    println("--------------------------------------------------------------------------------------------------------------")

    //------------------------------------------------------------------------------------------------------------------
    // Homework
    //------------------------------------------------------------------------------------------------------------------

    // read data
    val region = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .option("sep", ";")
      .csv(s"$path/tpch_region.csv")
      .as[(String, String, String)]

    val nation = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .option("sep", ";")
      .csv(s"$path/tpch_nation.csv")
      .as[(String, String, String, String)]

    val customer = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .option("sep", ";")
      .csv(s"$path/tpch_customer.csv")
      .as[(String, String, String, String, String, String, String, String)]

    val lineitem = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .option("sep", ";")
      .csv(s"$path/tpch_lineitem.csv")
      .as[(String, String, String, String, String, String, String, String,
      String, String, String, String, String, String, String, String)]

    val part = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .option("sep", ";")
      .csv(s"$path/tpch_part.csv")
      .as[(String, String, String, String, String, String, String, String, String)]

    val supplier = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .option("sep", ";")
      .csv(s"$path/tpch_supplier.csv")
      .as[(String, String, String, String, String, String, String)]

    val orders = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .option("sep", ";")
      .csv(s"$path/tpch_orders.csv")
      .as[(String, String, String, String, String, String, String, String, String)]

    val nation_cols = nation.columns
    val region_cols = region.columns
    val customer_cols = customer.columns
    val lineitem_cols = lineitem.columns
    val part_cols = part.columns
    val supplier_cols = supplier.columns
    val orders_cols = orders.columns

    val customer_tuples = customer
      .flatMap(tuple2 =>
        Seq(
          tuple2._1->Seq(customer_cols(0)),
          tuple2._2->Seq(customer_cols(1)),
          tuple2._3->Seq(customer_cols(2)),
          tuple2._4->Seq(customer_cols(3)),
          tuple2._5->Seq(customer_cols(4)),
          tuple2._6->Seq(customer_cols(5)),
          tuple2._7->Seq(customer_cols(6)),
          tuple2._8->Seq(customer_cols(7)))
      )

    val supplier_tuples = supplier
      .flatMap(tuple2 =>
        Seq(
          tuple2._1->Seq(supplier_cols(0)),
          tuple2._2->Seq(supplier_cols(1)),
          tuple2._3->Seq(supplier_cols(2)),
          tuple2._4->Seq(supplier_cols(3)),
          tuple2._5->Seq(supplier_cols(4)),
          tuple2._6->Seq(supplier_cols(5)),
          tuple2._7->Seq(supplier_cols(6)))
      )

    val part_tuples = part
      .flatMap(tuple2 =>
        Seq(
          tuple2._1->Seq(part_cols(0)),
          tuple2._2->Seq(part_cols(1)),
          tuple2._3->Seq(part_cols(2)),
          tuple2._4->Seq(part_cols(3)),
          tuple2._5->Seq(part_cols(4)),
          tuple2._6->Seq(part_cols(5)),
          tuple2._7->Seq(part_cols(6)),
          tuple2._8->Seq(part_cols(7)),
          tuple2._9->Seq(part_cols(8)))
      )

    val orders_tuples = orders
      .flatMap(tuple2 =>
        Seq(
          tuple2._1->Seq(orders_cols(0)),
          tuple2._2->Seq(orders_cols(1)),
          tuple2._3->Seq(orders_cols(2)),
          tuple2._4->Seq(orders_cols(3)),
          tuple2._5->Seq(orders_cols(4)),
          tuple2._6->Seq(orders_cols(5)),
          tuple2._7->Seq(orders_cols(6)),
          tuple2._8->Seq(orders_cols(7)),
          tuple2._9->Seq(orders_cols(8)))
      )

    val lineitem_tuples = lineitem
      .flatMap(tuple2 =>
        Seq(
          tuple2._1->Seq(lineitem_cols(0)),
          tuple2._2->Seq(lineitem_cols(1)),
          tuple2._3->Seq(lineitem_cols(2)),
          tuple2._4->Seq(lineitem_cols(3)),
          tuple2._5->Seq(lineitem_cols(4)),
          tuple2._6->Seq(lineitem_cols(5)),
          tuple2._7->Seq(lineitem_cols(6)),
          tuple2._8->Seq(lineitem_cols(7)),
          tuple2._9->Seq(lineitem_cols(8)),
          tuple2._10->Seq(lineitem_cols(9)),
          tuple2._11->Seq(lineitem_cols(10)),
          tuple2._12->Seq(lineitem_cols(11)),
          tuple2._13->Seq(lineitem_cols(12)),
          tuple2._14->Seq(lineitem_cols(13)),
          tuple2._15->Seq(lineitem_cols(14)),
          tuple2._16->Seq(lineitem_cols(15)))
      )

    val region_tuples = region
      .flatMap(tuple2 =>
        Seq(
          tuple2._1->Seq(region_cols(0)),
          tuple2._2->Seq(region_cols(1)),
          tuple2._3->Seq(region_cols(2)))
      )

    ((((((nation
      // split records, create cells
      .flatMap(tuple1 =>
        Seq(
          tuple1._1->Seq(nation_cols(0)),
          tuple1._2->Seq(nation_cols(1)),
          tuple1._3->Seq(nation_cols(2)),
          tuple1._4->Seq(nation_cols(3)))
      ) union region_tuples) union customer_tuples) union lineitem_tuples) union orders_tuples)
      union part_tuples) union supplier_tuples)

      // preaggregation, concat column title for same value
      .groupByKey(value => value._1)
      .reduceGroups((cell1, cell2) => (cell1._1, cell1._2 ++ cell2._2))
      .map(_._2)
      .map(pair => (pair._1, pair._2.distinct))
      // TODO partition / distribute .repartition() ?

      // generate attribute sets
      .groupByKey(value => value._1)
      .reduceGroups((cell1, cell2) => (cell1._1, cell1._2 ++ cell2._2))
      .map(_._2)
      .map(pair => pair._2.distinct)
      .distinct()

      // generate inclusion lists
      .flatMap(attributeSet => {
          val set = attributeSet.toSet
          set.map(el => (el, (set - el).toSeq)).toSeq
      })

      // redistribute

      // aggregate, i.e., intersect inclusion list values
      .groupByKey(value => value._1)
      .reduceGroups((cell1, cell2) => (cell1._1, cell1._2.toSet.intersect(cell2._2.toSet).toSeq))
      .map(_._2)

      // remove empty sets
      .filter(_._2.nonEmpty)

      // split into INDs
      .foreach(pair => {
        println(pair._1 + " < " + pair._2.mkString(", "))
      })

      // return output

  }

}