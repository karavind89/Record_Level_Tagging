/**
  * Created by aravind on 11/15/16.
  */

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.functions._

object Hive_Tagging {

     case class Record(key: Int, value: String)

     def main(args: Array[String]) {

      val sparkConf = new SparkConf().setAppName("Record Level Tagging")
      val sc = new SparkContext(sparkConf)

      val hiveContext = new HiveContext(sc)
      import hiveContext.implicits._
      import hiveContext.sql

      //Extracting only instance_id, since we need only ID to be tagged
      val id_ldos = hiveContext.sql("select int(instance_id) as ID from ibdq.mm_ldos").rdd
      val id_warranty_type = hiveContext.sql("select int(instance_id) as ID from ibdq.mm_warranty_type").rdd

      //Tagging records with respective "values"
      val pairs1 = id_ldos.map(s => (s, "ldos"))
      val pairs2 = id_warranty_type.map(s => (s, "warranty_type"))


      val pair_result = sc.union(pairs1)

      val reducedByKey = pair_result.reduceByKey((x,y) => x + ","+ y)

      val split = reducedByKey.map( s => List(s._1) ++ List.fromArray(s._2.split(",")))

      val metrics = List("ldos","warranty_type")

      val temp = split.map( s => List(s.slice(0,1)) ++ List( (metrics.map( a => if ((s.slice(1,s.size)).contains(a)) "Y" else "N") ) ) )

      val result = temp.map( s => s.flatten )

      case class RLT (instance_id: String, ldos : String, warranty_type : String)

      val df = result.map( { case Row (instance_id: Int, ldos : String, warranty_type : String) => RLT (instance_id,ldos,warranty_type) } ).toDF("instance_id","ldos","warranty_type")

      df.registerTempTable("mytemptable")
      hiveContext.sql("insert into table ibdq.taggingscala select * from mytemptable");

      sc.stop()

  }
}
