package sql

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConversions._

object ReadProtoBuffFile {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Spark SQL Read Protobuf File")
      .config("spark.some.config.option", "some-value")
      .config("fs.hdfs.impl", classOf[DistributedFileSystem].getName)
      .config("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
      .config("google.cloud.auth.service.account.enable",true)
      .config("google.cloud.auth.service.account.json.keyfile","/Users/kuldeep.adhikari/service_account_qa1.json")
      .config("spark.master", "local")
      //   .config("spark.master", "yarn")
      .getOrCreate()
    val format = new SimpleDateFormat("yyyy-MM-dd")
    var runDate=format.format(Calendar.getInstance().getTime())
    var Env="GCPINT"
    if(args.length == 1){
      Env=args(0).toString
    }else if(args.length == 2){
      Env=args(0).toString
      runDate=args(1).toString
    }
    print(readProtoBuffFile(spark,runDate,Env,"999999001"))
    spark.stop()
  }

  def visitor_export_location(Env : String) :String={
    if(Env=="GCPQA"){
      "gs://use1-nonprod-eng-all-bucket/qa/tapflow/visitor_export"}
    else{
       "gs://use1-nonprod-eng-all-bucket/tapsync/int/visitor_export"
    }
  }
  // gcloud dataproc jobs submit spark --cluster=dataproc-hdp-cluster1-qa --region us-east1    --class sql.ReadProtoBuffFile --jars gs://mp-data-ingestion-v1/test/scala/sparkwriteparquetfile_2.11-0.1.jar
  def readProtoBuffFile(spark: SparkSession,runDate: String,Env: String,visitorId: String): Boolean = {
    val dspexport=spark.sparkContext.sequenceFile(visitor_export_location(Env)+"/visitor_export/date="+runDate+"/",classOf[org.apache.hadoop.io.BytesWritable],classOf[org.apache.hadoop.io.BytesWritable])
    //  val dspexport=spark.sparkContext.sequenceFile("gs://use1-nonprod-eng-all-bucket/qa/tapflow/visitor_export/visitor_export/date="+runDate+"/",classOf[org.apache.hadoop.io.BytesWritable],classOf[org.apache.hadoop.io.BytesWritable])
    val dspexportrecord=dspexport.map(x=>com.groupm.mplatform.MembershipProtos.VisitorExport.parseFrom(x._2.copyBytes))
    val d=dspexportrecord.filter(_.getId()==visitorId)
    println(d.take(2).toString)
    /*d.collect
res180: Array[com.groupm.mplatform.MembershipProtos.VisitorExport] =
Array(acids: 909
acids: 901
acids: 908
mpid: "qa999999001"
dsps {
  platformId: 15
  externalId: "299de81e-9b26-3f00-9fa2-b5fa21623677"
}
dsps {
  platformId: 0
  externalId: "specialchar\\`\\`!@#$%^&*()_+-={[}]|\\;;:\\\","
}
dsps {
  platformId: 3
  externalId: "299de81e-9b26-3f00-9fa2-b5fa21623677"
}
dsps {
  platformId: 156
  externalId: "299de81e-9b26-3f00-9fa2-b5fa21623677"
}
segments {
  acid: 908
  modelType: 4
  membership: "\300@\022"
}
segments {
  acid: 901
  modelType: 4
  membership: "@ \001\a\000\360\t"
}
id: "999999001"
idType: 0
)*/
    val membership=d.take(2).map(_.getSegmentsList.map(_.getMembership.toByteArray))
    /*var segments=d.map(_.getSegmentsList).collect
membership: Array[java.util.List[com.groupm.mplatform.MembershipProtos.VisitorExport.ModelTypeEntry]] =
Array([acid: 908
modelType: 4
membership: "\300@\022"
, acid: 901
modelType: 4
membership: "@ \001\a\000\360\t"
])

for(s<-segments){println(m.map(_.getMembership.toByteArray))}
ArrayBuffer([B@5c48342d, [B@6adab5e2)


*/
    println("membership"+ membership.toString)
    val bitSet1 = java.util.BitSet.valueOf(membership(0)(0).map(_.byteValue))
    val bitSet2 = java.util.BitSet.valueOf(membership(0)(1).map(_.byteValue))
    println(bitSet1)
    println(bitSet2)
/*scala> membership
res240: Array[scala.collection.mutable.Buffer[Array[Byte]]] = Array(ArrayBuffer(Array(-64, 64, 18), Array(64, 32, 1, 7, 0, -16, 9)))

scala> membership(0)(0).map(_.byteValue)
res241: Array[Byte] = Array(-64, 64, 18)

scala> java.util.BitSet.valueOf(membership(0)(0).map(_.byteValue))
res242: java.util.BitSet = {6, 7, 14, 17, 20}*/

    val dspexportmetadata=spark.read.json(visitor_export_location(Env)+"/visitor_export/metadata/date="+runDate+"/metadata")
    //    val dspexportmetadata=spark.read.json("visitor_export/metadata/date=2020-02-18/metadata")
    println(dspexportmetadata.collect())

    dspexportmetadata.createOrReplaceTempView("dspexportmetadata")
    val sqlDF = spark.sql("SELECT * FROM dspexportmetadata")
    sqlDF.show()


  /* val metadata=dspexportmetadata.filter($"accId"==="901" && $"modelingLevel"==="MOOKIE" && $"modelType"==="4").collect
    res181: Array[org.apache.spark.sql.Row] = Array([901,4,5287,MOOKIE,WrappedArray(1505774077, 4013, 4343, 4371, 4358, 4350, 4359, 4349, 4348, 4092, 3918, 3916, 4091, 4051, 4021, 4011, 4012, 4004, 874994764, 979131174, 4401, 4402, 1547421600, 4144, 4578, 3822, 3832, 4199, 4308, 4405, 4424, 4765, 4193, 4295, 4398, 4421, 4304, 4653, 4173, 4175, 4172, 4149, 1532, 597489316, 4140, 4134, 4397, 4139, 4142, 168484990, 4341, 4379, 4353, 4368, 4321, 4661)])

    for(aa<-bitSet1){segments(j)=objectArray(aa).toString.toInt
      j=j+1}


    */
    /*scala> metadata
    res301: Array[org.apache.spark.sql.Row] = Array([901,4,5287,MOOKIE,WrappedArray(1505774077, 4013, 4343, 4371, 4358, 4350, 4359, 4349, 4348, 4092, 3918, 3916, 4091, 4051, 4021, 4011, 4012, 4004, 874994764, 979131174, 4401, 4402, 1547421600, 4144, 4578, 3822, 3832, 4199, 4308, 4405, 4424, 4765, 4193, 4295, 4398, 4421, 4304, 4653, 4173, 4175, 4172, 4149, 1532, 597489316, 4140, 4134, 4397, 4139, 4142, 168484990, 4341, 4379, 4353, 4368, 4321, 4661)])

    scala> val m=metadata(0).getList(4).toArray.map(_.toString.toInt)
    m: Array[Int] = Array(1505774077, 4013, 4343, 4371, 4358, 4350, 4359, 4349, 4348, 4092, 3918, 3916, 4091, 4051, 4021, 4011, 4012, 4004, 874994764, 979131174, 4401, 4402, 1547421600, 4144, 4578, 3822, 3832, 4199, 4308, 4405, 4424, 4765, 4193, 4295, 4398, 4421, 4304, 4653, 4173, 4175, 4172, 4149, 1532, 597489316, 4140, 4134, 4397, 4139, 4142, 168484990, 4341, 4379, 4353, 4368, 4321, 4661)

    scala> val membershipIndex=bitSet1.stream.toArray
    membershipIndex: Array[Int] = Array(6, 7, 14, 17, 20)

    scala> membershipIndex.foreach(x=>println(m(x)))
    4359
    4349
    4021
    4004
    4401
*/


    if(d.count()>=1){
      return true
    }else{
      return false
    }


  }

}
