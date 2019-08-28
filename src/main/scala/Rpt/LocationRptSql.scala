package Rpt

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object LocationRptSql {
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("目录参数不正确，退出程序")
      sys.exit()
    }
    // 创建一个集合保存输入和输出目录
    val Array(inputPath) = args
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
    val df = spark.read.parquet(inputPath)
    df.createOrReplaceTempView("dmp_table")
    spark.sql("select provincename,cityname,sum(case when requestmode=1 and processnode>=1 then 1 else 0 end) originalNumber," +
      "sum(case when requestmode=1 and processnode>=2 then 1 else 0 end) effectiveNumber," +
      "sum(case when requestmode=1 and processnode=3 then 1 else 0 end ) AdNumber, " +
      "sum(case when iseffective=1 and isbilling =1 and isbid=1 then 1 else 0 end) haveNumber," +
      "sum(case when iseffective=1 and isbilling =1 and iswin=1 and adorderid!=0 then 1 else 0 end) sucessNumber," +
      "sum(case when requestmode=2 and iseffective=1 then 1 else 0 end) showNumber," +
      "sum(case when requestmode=3 and iseffective=1 then 1 else 0 end) clickNumber," +
      "sum(case when iseffective=1 and isbilling =1 and iswin=1 then WinPrice/1000 else 0 end ) winprice," +
      "sum(case when iseffective=1 and isbilling =1 and iswin=1 then adpayment/1000 else 0 end ) adpay from dmp_table group by provincename,cityname ")
    spark.sql("select *from dmp_table").show()
    spark.stop()
  }

}
