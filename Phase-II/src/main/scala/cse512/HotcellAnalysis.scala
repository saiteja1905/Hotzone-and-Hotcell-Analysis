package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
{
  // Load the original data from a data source
  var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
  pickupInfo.createOrReplaceTempView("nyctaxitrips")
  pickupInfo.show()

  // Assign cell coordinates based on pickup points
  spark.udf.register("CalculateX",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 0)
    )))
  spark.udf.register("CalculateY",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 1)
    )))
  spark.udf.register("CalculateZ",(pickupTime: String)=>((
    HotcellUtils.CalculateCoordinate(pickupTime, 2)
    )))
  pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
  var newCoordinateName = Seq("x", "y", "z")
  pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
  pickupInfo.show()

  // Define the min and max of x, y, z
  val minX = -74.50/HotcellUtils.coordinateStep
  val maxX = -73.70/HotcellUtils.coordinateStep
  val minY = 40.50/HotcellUtils.coordinateStep
  val maxY = 40.90/HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31
  val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

  // YOU NEED TO CHANGE THIS PART
  pickupInfo.createOrReplaceTempView("nyctaxitripscoordinates")
  pickupInfo = spark.sql("select x,y,z from nyctaxitripscoordinates where x BETWEEN " + minX + " AND " + maxX + " and y BETWEEN " + minY + " AND " + maxY + " and z BETWEEN " + minZ + " AND " + maxZ + " order by z,y,x")

  pickupInfo.createOrReplaceTempView("filterednyctaxitrips")

  pickupInfo = spark.sql("select x, y, z, count(*) as hotCells from filterednyctaxitrips group by x, y, z order by z,y,x")

  pickupInfo.createOrReplaceTempView("finalHotCells")

  val aggregateOfFinalHotCellValues = spark.sql("select sum(hotCells) as aggregateOfFinalHotCellValues from finalHotCells")
  aggregateOfFinalHotCellValues.createOrReplaceTempView("aggregateOfFinalHotCellValues")

  val meanValue = (aggregateOfFinalHotCellValues.first().getLong(0).toDouble / numCells.toDouble).toDouble

  val squaredHotCells = spark.sql("select SUM(hotCells*hotCells) as squaredHotCells from finalHotCells")
  squaredHotCells.createOrReplaceTempView("squaredHotCells")

  val standardDeviationValue = scala.math.sqrt(((squaredHotCells.first().getLong(0).toDouble / numCells.toDouble) - (meanValue.toDouble * meanValue.toDouble))).toDouble

  spark.udf.register("neighborCount", (x: Int, y: Int, z: Int, minX: Int, maxX: Int, minY: Int, maxY: Int, minZ: Int, maxZ: Int) => ((HotcellUtils.neighborCount(x, y, z, minX, minY, minZ, maxX, maxY, maxZ))))

  val neighbouringCells = spark.sql("select neighborCount(hc1.x, hc1.y, hc1.z, " + minX + "," + maxX + "," + minY + "," + maxY + "," + minZ + "," + maxZ + ") as neighbourCellNumber,"
    + "hc1.x as x, hc1.y as y, hc1.z as z, sum(hc2.hotCells) as aggregateOfHotCells "
    + "from finalHotCells as hc1, finalHotCells as hc2 "
    + "where hc2.x BETWEEN hc1.x-1 AND hc1.x+1 "
    + "and hc2.y BETWEEN hc1.y-1 AND hc1.y+1 "
    + "and hc2.z BETWEEN hc1.z-1 AND hc1.z+1 "
    + "group by hc1.z, hc1.y, hc1.x "
    + "order by hc1.z, hc1.y, hc1.x")
  neighbouringCells.createOrReplaceTempView("neighbouringCells")

  spark.udf.register("scoreOfG", (neighbourCellNumber: Int, aggregateOfHotCells: Int, numCells: Int, x: Int, y: Int, z: Int, meanValue: Double, standardDeviationValue: Double) => ((HotcellUtils.scoreOfG(neighbourCellNumber, aggregateOfHotCells, numCells, x, y, z, meanValue, standardDeviationValue))))

  pickupInfo = spark.sql("select scoreOfG(neighbourCellNumber, aggregateOfHotCells, "+ numCells + ", x, y, z," + meanValue + ", " + standardDeviationValue + ") as scoreOfG, x, y, z from neighbouringCells");
  pickupInfo.createOrReplaceTempView("gScoreView")

  pickupInfo = spark.sql("select x, y, z from gScoreView order by scoreOfG desc")
  pickupInfo.createOrReplaceTempView("finalPickupInfo")
  return pickupInfo.coalesce(1)
}
}