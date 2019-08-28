package Rpt

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Dataset, SparkSession}

object MediaCtSql {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("目录参数不正确，退出程序")
      sys.exit()
    }
    // 创建一个集合保存输入和输出目录
    val Array(inputPath,inputPath1) = args
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      // 设置序列化方式 采用Kyro序列化方式，比默认序列化方式性能高
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    // 创建执行入口
    val spark = SparkSession.builder().config(conf)
      .config("spark.sql.warehouse.dir", "D://spark-warehouse")
      // 设置压缩方式 使用Snappy方式进行压缩
      .config("spark.sql.parquet.compression.codec", "snappy")
      .enableHiveSupport()
      .getOrCreate()

    // 进行数据的读取，处理分析数据
    val df = spark.read.textFile(inputPath)
    import spark.implicits._
    val res = df.filter(_.length >= 5).map(x => {
      val strings = x.split("\t")
      (strings(4), strings(1))
    })
    val map: Map[String, String] = res.collect().toMap
    val broadcast: Broadcast[Map[String, String]] = spark.sparkContext.broadcast(map)
    val dmp = spark.read.parquet(inputPath1)
    dmp.createOrReplaceTempView("dmp_table")
    spark.udf.register("getname",(appid:String)=>{
      map.getOrElse(appid,"暂无")
    })
    val frame = spark.sql("select name," +
      "sum(case when requestmode=1 and processnode>=1 then 1 else 0 end) originalNumber," +
      "sum(case when requestmode=1 and processnode>=2 then 1 else 0 end) effectiveNumber," +
      "sum(case when requestmode=1 and processnode=3 then 1 else 0 end ) AdNumber, " +
      "sum(case when iseffective=1 and isbilling =1 and isbid=1 then 1 else 0 end) haveNumber," +
      "sum(case when iseffective=1 and isbilling =1 and iswin=1 and adorderid!=0 then 1 else 0 end) sucessNumber," +
      "sum(case when requestmode=2 and iseffective=1 then 1 else 0 end) showNumber," +
      "sum(case when requestmode=3 and iseffective=1 then 1 else 0 end) clickNumber," +
      "sum(case when iseffective=1 and isbilling =1 and iswin=1 then WinPrice/1000 else 0 end ) winprice," +
      "sum(case when iseffective=1 and isbilling =1 and iswin=1 then adpayment/1000 else 0 end ) adpay from "+
      "(select *,(case when appname='其他' then getname(appid) else appname end) name from dmp_table)tb group by tb.name " )
    val load=ConfigFactory.load()
    val prop=new Properties()
    prop.setProperty("user",load.getString("jdbc.user"))
    prop.setProperty("password",load.getString("jdbc.password"))
    frame.write.mode("append").jdbc(load.getString("jdbc.url"),load.getString("jdbc.tablename5"),prop)
    spark.stop()
  }
}
