package Tags

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
import utils.Tag

object TagProvince extends Tag{
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    val row = args(0).asInstanceOf[Row]
    //设备运营商方式
    //移 动
    val province = row.getAs[String]("provincename")
    if(StringUtils.isNotBlank(province)){
      list:+=("ZP"+province,1)
    }
    list
  }
}
