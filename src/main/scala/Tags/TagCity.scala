package Tags

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
import utils.Tag

object TagCity extends Tag{
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    val row = args(0).asInstanceOf[Row]
    val city = row.getAs[String]("cityname")
    if(StringUtils.isNotBlank(city)){
      list:+=("ZC"+city,1)
    }
    list
  }
}
