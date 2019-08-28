package ProCityCt

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object ProCityCt {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("目录参数不正确，退出程序")
      sys.exit()
    }
    // 创建一个集合保存输入和输出目录
    val Array(inputPath, outputPath) = args
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
    val frame = spark.read.parquet(inputPath)
    frame.createOrReplaceTempView("tb_dmp")
    val result = spark.sql("select count(1) ct, provincename,cityname from tb_dmp group by provincename,cityname order by ct desc")
    result.coalesce(1).write.partitionBy("provincename","cityname").json(outputPath)
   //val load=ConfigFactory.load()
   //val prop=new Properties()
   //prop.setProperty("user",load.getString("jdbc.user"))
   //prop.setProperty("password",load.getString("jdbc.password"))
   //result.write.mode("append").jdbc(load.getString("jdbc.url"),load.getString("jdbc:tablename"),prop)
    spark.stop()
  }
}
