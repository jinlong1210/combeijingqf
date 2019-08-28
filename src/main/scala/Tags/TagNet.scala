package Tags

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
import utils.Tag

object TagNet extends Tag{
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    val row = args(0).asInstanceOf[Row]
   //WIFI D00020001 4G D00020002
   //3G D00020003
   //2G D00020004
   //_   D00020005

    val netname = row.getAs[String]("networkmannername")
    if(StringUtils.isNotBlank(netname)){
      if("Wifi".equals(netname)) {
        list :+= ("WIFI D00020001", 1)
      }else if("4G".equals(netname)){
        list :+= ("4G D00020002", 1)
      } else if("3G".equals(netname)){
        list :+= ("3G D00020003", 1)
      }else if("2G".equals(netname)){
        list :+= ("2G D00020004", 1)
      }else{
        list :+= ("_   D00020005", 1)
      }
    }
    list
  }
}
