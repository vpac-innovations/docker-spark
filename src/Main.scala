package au.com.vpacinnovations.spark

import org.apache.spark.{SparkContext, SparkConf}
import scala.util.Try

import ucar.nc2.{NetcdfFile, NetcdfFileWriter, Dimension, Variable}
import ucar.ma2.DataType
import ucar.ma2.{Array => netcdfArray}

object SparkTestApp {

    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("test").setMaster("spark://master:7077")
        val sc = new SparkContext(conf)

        val inpath = "LGA_tile_x0_y0.nc"
        val infileTry = Try(NetcdfFile.openInmemory(inpath))
        if (!infileTry.isSuccess) { 
            sc.stop()
            System.exit(1)
        }
/**
        val file = sc.textFile("Mean_Ann_Temp_100_Vic.asc")
        val file = sc.textFile("hdfs:/Mean_Ann_Temp_100_Vic.asc")
**/
        val file = infileTry.get
        val header = file.take(6)

        val data = file.filter(x => !header.contains(x))

         val values = data.map(x => x.split(" "))

         val sums = values.map(x => x.map(_.toDouble).reduce(_+_))

         val total = sums.reduce(_+_)

         println(s"Total summary of the point values are $total")
    }
}
