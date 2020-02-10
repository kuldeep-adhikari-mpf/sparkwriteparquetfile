package sql
import java.text.SimpleDateFormat
import java.time.Instant
import java.util.Calendar

import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types._

object SparkSQLExample {

  case class Person(name: String, age: Long)


  def main(args: Array[String]): Unit = {

  /*  if (args.length != 1) {
      throw new IllegalArgumentException(
        "Exactly 1 arguments are required: <outputPath>")
    }
*/
   // val inputPath = args(0)
  //  val outputPath = args(0)

    // $example on:init_session$
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .config("spark.master", "local")
      .getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
    // $example off:init_session$
   // runMerge(spark)
    runDataPartnerWriteParquetFile(spark)
    //   runBasicDataFrameExampleWriteParquetFile(spark)
    //runBasicDataFrameExample(spark)
  //  runDatasetCreationExample(spark)
 //   runInferSchemaExample(spark)
 //   runProgrammaticSchemaExample(spark)

    spark.stop()
  }

  private def runDataPartnerWriteParquetFile(spark: SparkSession): Unit = {
    var epochrunDate = Instant.now.toEpochMilli
    var epochexpireDate = Instant.now.plusSeconds(864000).toEpochMilli //+10 days expire time
    val format = new SimpleDateFormat("ddMMyyy")
    val ingestionDate=format.format(Calendar.getInstance().getTime())
    val uploadFolderName="gs://mp-data-ingestion-v1/test/" + ingestionDate +""
    val sourcefolderName="test/" + ingestionDate +"/"

    val someData = Seq(
      Row(901, 0,"10001"+epochrunDate,1,2,"testa","testavalue",new java.sql.Timestamp(epochrunDate),null,"V","1","0",1),
      Row(901, 0,"10001"+epochrunDate,1,2,null,null,new java.sql.Timestamp(epochrunDate),null,"E","1","0",1),
      Row(901, 0,"10002"+epochrunDate,1,2,"testa","testavalue",new java.sql.Timestamp(epochrunDate),new java.sql.Timestamp(23432L),"V","1","0",1),
      Row(901, 0,"10003"+epochrunDate,10678,371631,null,null,new java.sql.Timestamp(epochrunDate),new java.sql.Timestamp(epochexpireDate),"E","0","4",1),
      Row(901, 0,"10003"+epochrunDate,10678,371631,"testa1","testavalue1epochrunDate",new java.sql.Timestamp(epochrunDate),new java.sql.Timestamp(epochexpireDate),"V","0","4",1),
      Row(901, 0,"10004"+epochrunDate,10678,371631,"test2","testavalue1epochrunDate",new java.sql.Timestamp(epochrunDate),new java.sql.Timestamp(epochexpireDate),"V","0","4",1),
      Row(901, 0,"10005"+epochrunDate,10678,371631,"test3","testavalue2",new java.sql.Timestamp(epochrunDate),new java.sql.Timestamp(epochexpireDate),"V","0","4",1),
      Row(901, 0,"10006"+epochrunDate,10678,371631,"test4","testavalue2",new java.sql.Timestamp(epochrunDate),new java.sql.Timestamp(epochexpireDate),"V","0","4",1),
      Row(901, 0,"10007"+epochrunDate,774847,381268,null,null,new java.sql.Timestamp(epochrunDate),new java.sql.Timestamp(epochexpireDate),"E","0","4",1),
      Row(901, 0,"10007"+epochrunDate,774847,381268,"TapSyncActivityAutoTest_MOOKIE20191104","testavalue1epochrunDate",new java.sql.Timestamp(epochrunDate),new java.sql.Timestamp(epochexpireDate),"V","0","4",1),
      Row(901, 0,"10008"+epochrunDate,10678,380947,null,null,new java.sql.Timestamp(epochrunDate),new java.sql.Timestamp(epochexpireDate),"E","0","4",1),
      Row(901, 0,"10009"+epochrunDate,993761,993761,null,null,new java.sql.Timestamp(epochrunDate),new java.sql.Timestamp(epochexpireDate),"E","0","4",1),
      Row(901, 0,"10010"+epochrunDate,993761,993761,"age","20",new java.sql.Timestamp(epochrunDate),new java.sql.Timestamp(epochexpireDate),"V","0","4",1),
      Row(901, 0,"10011"+epochrunDate,993761,993761,"age","30",new java.sql.Timestamp(epochrunDate),new java.sql.Timestamp(epochexpireDate),"V","0","4",1),
      Row(901, 0,"10012"+epochrunDate,993761,993761,"gender","male",new java.sql.Timestamp(epochrunDate),new java.sql.Timestamp(epochexpireDate),"V","0","4",1)
    )
    epochrunDate = Instant.now.toEpochMilli
    val someData903 = Seq(
      Row(903, 0,"10001"+epochrunDate,1,2,"testa","testavalue",new java.sql.Timestamp(epochrunDate),null,"V","1","0",1),
      Row(903, 0,"10002"+epochrunDate,1,2,"testa","testavalue",new java.sql.Timestamp(epochrunDate),new java.sql.Timestamp(23432L),"V","1","0",1),
      Row(903, 1,"10003"+epochrunDate,10678,371631,"testa11","testavalue1epochrunDate",new java.sql.Timestamp(epochrunDate),new java.sql.Timestamp(epochexpireDate),"V","0","4",1),
      Row(903, 0,"10004"+epochrunDate,10678,371631,"test2","testavalue1epochrunDate",new java.sql.Timestamp(epochrunDate),new java.sql.Timestamp(epochexpireDate),"V","0","4",1),
      Row(903, 0,"10005"+epochrunDate,10678,371631,"test3","testavalue2",new java.sql.Timestamp(epochrunDate),new java.sql.Timestamp(epochexpireDate),"V","0","4",1),
      Row(903, 0,"10006"+epochrunDate,10678,371631,"test4","testavalue2",new java.sql.Timestamp(epochrunDate),null,"V","0","4",1)

    )
    epochrunDate = Instant.now.toEpochMilli
    val someData902 = Seq(
      Row(902, 0,"10001"+epochrunDate,1,2,"testa","testavalue",new java.sql.Timestamp(epochrunDate),null,"V","1","0",1),
      Row(902, 0,"10002"+epochrunDate,1,2,"testa","testavalue",new java.sql.Timestamp(epochrunDate),new java.sql.Timestamp(epochexpireDate),"V","1","0",1),
      Row(902, 1,"10003"+epochrunDate,10678,371631,"testa11","testavalue1epochrunDate畫",new java.sql.Timestamp(epochrunDate),new java.sql.Timestamp(epochexpireDate),"X","0","4",1),
      Row(902, 0,"10004"+epochrunDate,10678,371631,"test2","testavalue1epochrunDate観",new java.sql.Timestamp(epochrunDate),new java.sql.Timestamp(epochexpireDate),"V","0","4",1),
      Row(902, 0,"10005"+epochrunDate,10678,371631,"test3Simplified Chinese汉字","testavalue2UnicodeâæëĄNewǼ漢字,chinese汉字",new java.sql.Timestamp(epochrunDate),new java.sql.Timestamp(epochexpireDate),"V","2","4",1),
      Row(902, 0,"10006"+epochrunDate,10678,371631,"test3Simplified Chinese汉字","testavalue2UnicodeâæëĄNewǼ漢字,chinese汉字",new java.sql.Timestamp(epochrunDate),new java.sql.Timestamp(epochexpireDate),"V","3","4",1),
      Row(902, 0,"10007"+epochrunDate,10678,371631,"test3Simplified Chinese汉字","testavalue2UnicodeâæëĄNewǼ漢字,chinese汉字",new java.sql.Timestamp(epochrunDate),new java.sql.Timestamp(epochexpireDate),"V","3","4",1)
    )

    val someSchema = List(
      StructField("account_id",IntegerType,true),
      StructField("campaign_id",IntegerType,true),
      StructField("visitor_id",StringType,true),
      StructField("data_partner_id",IntegerType,true),
      StructField("data_source_id",IntegerType,true),
      StructField("attribute_name",StringType,true),
      StructField("attribute_value",StringType,true),
      StructField("time_stamp",TimestampType,true),
      StructField("expiration_time_stamp",TimestampType,true),
      StructField("fact_type",StringType,true),
      StructField("id_type",StringType,true),
      StructField("device_type",StringType,true),
      StructField("event_count",IntegerType,true)
    )

    val someDF901 = spark.createDataFrame(
      spark.sparkContext.parallelize(someData),
      StructType(someSchema)
    )
    someDF901.write.mode(SaveMode.Overwrite).parquet(sourcefolderName+"DataPartner1.901.parquet."+epochrunDate)
    println("{\"DataFileLocation\":\""+uploadFolderName+"/DataPartner1.901.parquet."+epochrunDate+"/\",\"account_id\":901}\"")

    val someDF903 = spark.createDataFrame(
      spark.sparkContext.parallelize(someData903),
      StructType(someSchema)
    )

    someDF903.write.mode(SaveMode.Overwrite).parquet(sourcefolderName+"DataPartner1.903.parquet."+epochrunDate)
    println("{\"DataFileLocation\":\""+uploadFolderName+"/DataPartner1.903.parquet."+epochrunDate+"/\",\"account_id\":903}\"")

    val someDF902 = spark.createDataFrame(
      spark.sparkContext.parallelize(someData902),
      StructType(someSchema)
    )

    someDF902.write.mode(SaveMode.Overwrite).parquet(sourcefolderName+"DataPartner1.902.parquet."+epochrunDate)
    println("{\"DataFileLocation\":\""+uploadFolderName+"/DataPartner1.902.parquet."+epochrunDate+"/\",\"account_id\":902}\"")

    someDF901.createOrReplaceTempView("DataPartner")

    val sqlDF = spark.sql("SELECT * FROM DataPartner")
    sqlDF.show()

    epochrunDate = Instant.now.toEpochMilli
    val someDataBadSchema902 = Seq(
      Row(902, 0,"10001"+epochrunDate,1,2,"testa","testavalue",new java.sql.Timestamp(epochrunDate),null,"V","1"),
      Row(902, 0,"10002"+epochrunDate,1,2,"testa","testavalue",new java.sql.Timestamp(epochrunDate),new java.sql.Timestamp(epochexpireDate),"V","1"),
      Row(902, 1,"10003"+epochrunDate,10678,371631,"testa11","testavalue1epochrunDate畫",new java.sql.Timestamp(epochrunDate),new java.sql.Timestamp(epochexpireDate),"X","0"),
      Row(902, 0,"10004"+epochrunDate,10678,371631,"test2","testavalue1epochrunDate観",new java.sql.Timestamp(epochrunDate),new java.sql.Timestamp(epochexpireDate),"V","0"),
      Row(902, 0,"10005"+epochrunDate,10678,371631,"test3Simplified Chinese汉字","testavalue2UnicodeâæëĄNewǼ漢字,chinese汉字",new java.sql.Timestamp(epochrunDate),new java.sql.Timestamp(epochexpireDate),"V","2"),
      Row(902, 0,"10006"+epochrunDate,10678,371631,"test3Simplified Chinese汉字","testavalue2UnicodeâæëĄNewǼ漢字,chinese汉字",new java.sql.Timestamp(epochrunDate),new java.sql.Timestamp(epochexpireDate),"V","3"),
      Row(902, 0,"10007"+epochrunDate,10678,371631,"test3Simplified Chinese汉字","testavalue2UnicodeâæëĄNewǼ漢字,chinese汉字",new java.sql.Timestamp(epochrunDate),new java.sql.Timestamp(epochexpireDate),"V","3")
    )

    val someSchemaBad = List(
      StructField("account_id",IntegerType,true),
      StructField("campaign_id",IntegerType,true),
      StructField("visitor_id",StringType,true),
      StructField("data_partner_id",IntegerType,true),
      StructField("data_source_id",IntegerType,true),
      StructField("attribute_name",StringType,true),
      StructField("attribute_value",StringType,true),
      StructField("time_stamp",TimestampType,true),
      StructField("expiration_time_stamp",TimestampType,true),
      StructField("fact_type",StringType,true),
      StructField("idtypewrong",StringType,true)
    )

    val someDF902BadSchema = spark.createDataFrame(
      spark.sparkContext.parallelize(someDataBadSchema902),
      StructType(someSchemaBad)
    )
    someDF902BadSchema.write.mode(SaveMode.Overwrite).parquet(sourcefolderName+"DataPartner1.902.badschema.parquet."+epochrunDate)
    println("{\"DataFileLocation\":\""+uploadFolderName+"/DataPartner1.902.badschema.parquet."+epochrunDate+"/\",\"account_id\":902}\"")
    someDF902BadSchema.createOrReplaceTempView("DataPartner")

    val sqlDF1 = spark.sql("SELECT * FROM DataPartner")
    sqlDF1.show()
  }

  private def runBasicDataFrameExampleWriteParquetFile(spark: SparkSession): Unit = {
    // Encoders for most common types are automatically provided by importing spark.implicits._
    import spark.implicits._

    val peopleDF = spark.read.json("src/main/resources/people.json")

    // DataFrames can be saved as Parquet files, maintaining the schema information
    peopleDF.write.parquet("people.parquet")

    // Read in the parquet file created above
    // Parquet files are self-describing so the schema is preserved
    // The result of loading a Parquet file is also a DataFrame
    val parquetFileDF = spark.read.parquet("people.parquet")

    // Parquet files can also be used to create a temporary view and then used in SQL statements
    parquetFileDF.createOrReplaceTempView("parquetFile")
    val namesDF = spark.sql("SELECT name FROM parquetFile WHERE age BETWEEN 13 AND 19")
    namesDF.map(attributes => "Name: " + attributes(0)).show()
    // +------------+
    // |       value|
    // +------------+
    // |Name: Justin|
    // +------------+
  }

  private def runBasicDataFrameExample(spark: SparkSession): Unit = {
    // $example on:create_df$
    val df = spark.read.json("src/main/resources/people.json")

    // Displays the content of the DataFrame to stdout
    df.show()
    // +----+-------+
    // | age|   name|
    // +----+-------+
    // |null|Michael|
    // |  30|   Andy|
    // |  19| Justin|
    // +----+-------+
    // $example off:create_df$

    // $example on:untyped_ops$
    // This import is needed to use the $-notation
    import spark.implicits._
    // Print the schema in a tree format
    df.printSchema()
    // root
    // |-- age: long (nullable = true)
    // |-- name: string (nullable = true)

    // Select only the "name" column
    df.select("name").show()
    // +-------+
    // |   name|
    // +-------+
    // |Michael|
    // |   Andy|
    // | Justin|
    // +-------+

    // Select everybody, but increment the age by 1
    df.select($"name", $"age" + 1).show()
    // +-------+---------+
    // |   name|(age + 1)|
    // +-------+---------+
    // |Michael|     null|
    // |   Andy|       31|
    // | Justin|       20|
    // +-------+---------+

    // Select people older than 21
    df.filter($"age" > 21).show()
    // +---+----+
    // |age|name|
    // +---+----+
    // | 30|Andy|
    // +---+----+

    // Count people by age
    df.groupBy("age").count().show()
    // +----+-----+
    // | age|count|
    // +----+-----+
    // |  19|    1|
    // |null|    1|
    // |  30|    1|
    // +----+-----+
    // $example off:untyped_ops$

    // $example on:run_sql$
    // Register the DataFrame as a SQL temporary view
    df.createOrReplaceTempView("people")

    val sqlDF = spark.sql("SELECT * FROM people")
    sqlDF.show()
    // +----+-------+
    // | age|   name|
    // +----+-------+
    // |null|Michael|
    // |  30|   Andy|
    // |  19| Justin|
    // +----+-------+
    // $example off:run_sql$

    // $example on:global_temp_view$
    // Register the DataFrame as a global temporary view
    df.createGlobalTempView("people")

    // Global temporary view is tied to a system preserved database `global_temp`
    spark.sql("SELECT * FROM global_temp.people").show()
    // +----+-------+
    // | age|   name|
    // +----+-------+
    // |null|Michael|
    // |  30|   Andy|
    // |  19| Justin|
    // +----+-------+

    // Global temporary view is cross-session
    spark.newSession().sql("SELECT * FROM global_temp.people").show()
    // +----+-------+
    // | age|   name|
    // +----+-------+
    // |null|Michael|
    // |  30|   Andy|
    // |  19| Justin|
    // +----+-------+
    // $example off:global_temp_view$
  }

  private def runDatasetCreationExample(spark: SparkSession): Unit = {
    import spark.implicits._
    // $example on:create_ds$
    // Encoders are created for case classes
    val caseClassDS = Seq(Person("Andy", 32)).toDS()
    caseClassDS.show()
    // +----+---+
    // |name|age|
    // +----+---+
    // |Andy| 32|
    // +----+---+

    // Encoders for most common types are automatically provided by importing spark.implicits._
    val primitiveDS = Seq(1, 2, 3).toDS()
    primitiveDS.map(_ + 1).collect() // Returns: Array(2, 3, 4)

    // DataFrames can be converted to a Dataset by providing a class. Mapping will be done by name
    val path = "src/main/resources/people.json"
    val peopleDS = spark.read.json(path).as[Person]
    peopleDS.show()
    // +----+-------+
    // | age|   name|
    // +----+-------+
    // |null|Michael|
    // |  30|   Andy|
    // |  19| Justin|
    // +----+-------+
    // $example off:create_ds$
  }


  private def runInferSchemaExample(spark: SparkSession): Unit = {
    // $example on:schema_inferring$
    // For implicit conversions from RDDs to DataFrames
    import spark.implicits._

    // Create an RDD of Person objects from a text file, convert it to a Dataframe
    val peopleDF = spark.sparkContext
      .textFile("src/main/resources/people.txt")
      .map(_.split(","))
      .map(attributes => Person(attributes(0), attributes(1).trim.toInt))
      .toDF()
    // Register the DataFrame as a temporary view
    peopleDF.createOrReplaceTempView("people")

    // SQL statements can be run by using the sql methods provided by Spark
    val teenagersDF = spark.sql("SELECT name, age FROM people WHERE age BETWEEN 13 AND 19")

    // The columns of a row in the result can be accessed by field index
    teenagersDF.map(teenager => "Name: " + teenager(0)).show()
    // +------------+
    // |       value|
    // +------------+
    // |Name: Justin|
    // +------------+

    // or by field name
    teenagersDF.map(teenager => "Name: " + teenager.getAs[String]("name")).show()
    // +------------+
    // |       value|
    // +------------+
    // |Name: Justin|
    // +------------+

    // No pre-defined encoders for Dataset[Map[K,V]], define explicitly
    implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]
    // Primitive types and case classes can be also defined as
    // implicit val stringIntMapEncoder: Encoder[Map[String, Any]] = ExpressionEncoder()

    // row.getValuesMap[T] retrieves multiple columns at once into a Map[String, T]
    teenagersDF.map(teenager => teenager.getValuesMap[Any](List("name", "age"))).collect()
    // Array(Map("name" -> "Justin", "age" -> 19))
    // $example off:schema_inferring$
  }

  private def runProgrammaticSchemaExample(spark: SparkSession): Unit = {
    import spark.implicits._
    // $example on:programmatic_schema$
    // Create an RDD
    val peopleRDD = spark.sparkContext.textFile("src/main/resources/people.txt")

    // The schema is encoded in a string
    val schemaString = "name age"

    // Generate the schema based on the string of schema
    val fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)

    // Convert records of the RDD (people) to Rows
    val rowRDD = peopleRDD
      .map(_.split(","))
      .map(attributes => Row(attributes(0), attributes(1).trim))

    // Apply the schema to the RDD
    val peopleDF = spark.createDataFrame(rowRDD, schema)

    // Creates a temporary view using the DataFrame
    peopleDF.createOrReplaceTempView("people")

    // SQL can be run over a temporary view created using DataFrames
    val results = spark.sql("SELECT name FROM people")

    // The results of SQL queries are DataFrames and support all the normal RDD operations
    // The columns of a row in the result can be accessed by field index or by field name
    results.map(attributes => "Name: " + attributes(0)).show()
    // +-------------+
    // |        value|
    // +-------------+
    // |Name: Michael|
    // |   Name: Andy|
    // | Name: Justin|
    // +-------------+
    // $example off:programmatic_schema$
  }

  private def runMerge(spark: SparkSession ) : Unit={
    // This is used to implicitly convert an RDD to a DataFrame.
    import spark.implicits._

    // Create a simple DataFrame, store into a partition directory
    val squaresDF = spark.sparkContext.makeRDD(1 to 5).map(i => (i, i * i)).toDF("value", "square")
    squaresDF.write.parquet("data/test_table/key=1")

    // Create another DataFrame in a new partition directory,
    // adding a new column and dropping an existing column
    val cubesDF = spark.sparkContext.makeRDD(6 to 10).map(i => (i, i * i * i)).toDF("value", "cube")
    cubesDF.write.parquet("data/test_table/key=2")

    // Read the partitioned table
    val mergedDF = spark.read.option("mergeSchema", "true").parquet("data/test_table")
    mergedDF.printSchema()
    mergedDF.show()
    // The final schema consists of all 3 columns in the Parquet files together
    // with the partitioning column appeared in the partition directory paths
    // root
    //  |-- value: int (nullable = true)
    //  |-- square: int (nullable = true)
    //  |-- cube: int (nullable = true)
    //  |-- key: int (nullable = true)
  }

}
