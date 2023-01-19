package cse512
import org.apache.spark.sql.SparkSession
import scala.math.{pow, sqrt}

object SpatialQuery extends App{
  def ST_Within(pointString1:String, pointString2:String, distance:Double): Boolean = {
    // if the input is empty or null return false
    if (pointString1 == null || pointString1.isEmpty())
      return false
	
	if (pointString2 == null || pointString2.isEmpty())
	  return false
	  
	if (distance <= 0.00)
	  return false

    val ArrayListPoint1 = pointString1.split(",")
    var pointX1 = ArrayListPoint1(0).toDouble
    var pointY1 = ArrayListPoint1(1).toDouble

    val ArrayListPoint2 = pointString2.split(",")
    var pointX2 = ArrayListPoint2(0).toDouble
    var pointY2 = ArrayListPoint2(1).toDouble

    var euclidianDist = sqrt(pow(pointX1 - pointX2, 2) + pow(pointY1 - pointY2, 2))
    if (euclidianDist <= distance)
      return true
    else
      return false
  }
  def ST_Contains(queryRectangle: String, pointString: String): Boolean = {
      if (pointString == null || pointString.isEmpty())
        { return false }
	
      if (queryRectangle == null || queryRectangle.isEmpty())
        { return false }

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
	  
  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>((ST_Contains(queryRectangle, pointString))))

    val resultDf = spark.sql("select * from point where ST_Contains('"+arg2+"',point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    rectangleDf.createOrReplaceTempView("rectangle")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>((ST_Contains(queryRectangle, pointString))))

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((ST_Within(pointString1, pointString2, distance))))

    val resultDf = spark.sql("select * from point where ST_Within(point._c0,'"+arg2+"',"+arg3+")")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point1")

    val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    pointDf2.createOrReplaceTempView("point2")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((ST_Within(pointString1, pointString2, distance))))
    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
    resultDf.show()

    return resultDf.count()
  }
}
