package Tags

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
import utils.Tag


object TagClient extends Tag{
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    val row = args(0).asInstanceOf[Row]
    // 获取client，打标签
    //设备操作系统
    //1 Android D00010001
    //2 IOS D00010002
    //3 WinPhone D00010003
    //_ 其 他 D00010004
    val client = row.getAs[Int]("client")
    if(client!=null){
      if(client==1) {
        list :+= ("1 Android D00010001", 1)
      }else if(client==2){
        list :+= ("2 IOS D00010002", 1)
      }else if(client==3){
        list :+= ("3 WinPhone D00010003", 1)
      }else{
        list :+= ("_ 其 他 D00010004", 1)
      }
    }
    list
  }
}
