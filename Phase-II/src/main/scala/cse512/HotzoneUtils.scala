package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    val arr_point = pointString.split(',')
      val pointX = arr_point(0).toDouble
      val pointY = arr_point(1).toDouble
      val arr_rect = queryRectangle.split(',')
      val pointA = arr_rect(0).toDouble
      val pointB = arr_rect(1).toDouble
      val pointC = arr_rect(2).toDouble
      val pointD = arr_rect(3).toDouble
      if (pointX>=pointA && pointX<=pointC && pointY>=pointB && pointY<=pointD)
      {
        true
      }
      else
      {
        false
      }
  }
}