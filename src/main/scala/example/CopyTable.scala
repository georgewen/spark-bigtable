package example

import org.apache.hadoop.hbase.spark.datasources.{HBaseSparkConf, HBaseTableCatalog}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._


object CopyTable extends App {

  val appName = this.getClass.getSimpleName.replace("$", "")
  println(s"$appName Spark application is starting up...")

  //val (projectId, instanceId, fromTable, toTable) = parse(args)
  val projectId = "sound-jigsaw-332323"
  val instanceId = "gw-dataproc-bigtable"
  val toTable = "copytable"
   

  import org.apache.spark.sql.SparkSession
  val spark = SparkSession.builder().config("spark.hadoop.validateOutputSpecs", false).getOrCreate()
  println(s"Spark version: ${spark.version}")
  import spark.implicits._


  import com.google.cloud.bigtable.hbase.BigtableConfiguration
  val conf = BigtableConfiguration.configure(projectId, instanceId)
  import org.apache.hadoop.hbase.spark.HBaseContext

  // Creating HBaseContext explicitly to use the conf above
  // That's how to use command-line arguments for projectId and instanceId
  // Otherwise, we'd have to use hbase-site.xml
  // See HBaseSparkConf.USE_HBASECONTEXT option in hbase-connectors project
  new HBaseContext(spark.sparkContext, conf)


  // Creates a configuration JSON for a given table
  // Used for HBaseTableCatalog.tableCatalog option
  // to read from or write to a Bigtable table

  def createCatalogJSON(table: String): String = {
    s"""{
       |"table":{"namespace":"default", "name":"$table", "tableCoder":"PrimitiveType"},
       |"rowkey":"recordid",
       |"columns":{
       |  "recordid":{"cf":"rowkey", "col":"recordid", "type":"string"},
       |  "responseid":{"cf":"cf", "col":"responseid", "type":"int"},
       |  "name":{"cf":"cf", "col":"name", "type":"string"},
       |  "Response":{"cf":"cf2", "col":"Response", "type":"string"}
       |}
       |}""".stripMargin
  }

  // The HBaseTableCatalog options are described in the sources themselves only
  // Search for HBaseSparkConf.scala in https://github.com/apache/hbase-connectors

  //manually create dataframe for source data
    // val records = Seq(
    //   (8, "bat","george"),
    //   (64, "mouse","alice"),
    //   (-27, "horse","john")
    // ).toDF("count", "word","username")

  var df = spark.read.format("csv").option("header","true").load("gs://gw-dataproc-data-store/*.txt")
  //df = df.select($"recordid",$"name",$"responseid",concat(col("responseid") ,lit(".xml")).alias("filename"))
  df = df.withColumn("filename",concat($"responseid" ,lit(".xml")))
  df.show()

  //df.repartitionby('filename')

  var df2 = spark.read.format("csv").load("gs://gw-dataproc-data-store/*.xml")
  //df2 = df2.withColumn("filename",regexp_replace(input_file_name(), "file:///home/george/scala/java-docs-samples/bigtable/spark/", "")).withColumnRenamed("_c0","Response")
  df2 = df2.withColumn("filename",regexp_replace(input_file_name(), "gs://gw-dataproc-data-store/", "")).withColumnRenamed("_c0","Response")

  df2.show()

  //df2.repartitionby('filename')

//  val fullDf = df2.join(df, df("filename") === df2("filename"),"inner")//.drop("filename2").drop("filename")
  val fullDf = df2.join(df,"filename").drop("filename")

  fullDf.show()

  println(s"Writing records to $toTable")
  fullDf
    .write
    .format("org.apache.hadoop.hbase.spark")
    .option(HBaseTableCatalog.tableCatalog, createCatalogJSON(toTable))
    .save
  println(s"Writing to $toTable...DONE")

}
