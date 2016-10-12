package au.com.vpacinnovations.spark

import org.apache.spark.{SparkContext, SparkConf}
import scala.util.Try

import ucar.nc2.{NetcdfFile, NetcdfFileWriter, Dimension, Variable, Attribute}
import ucar.nc2.dataset.NetcdfDataset
import ucar.ma2
import ucar.ma2.DataType
import ucar.ma2.{Array => netcdfArray}
import org.slf4j.Logger

object SparkTestApp {

    def main(args: Array[String]) {
        val LOG: Logger = org.slf4j.LoggerFactory.getLogger(this.getClass)

        val conf = new SparkConf().setAppName("test").setMaster("spark://master:7077")
        val sc = new SparkContext(conf)

        val inpath = "<TEST_NETCDF_FILE>.nc"
        val dataset = NetcdfDataset.openDataset(inpath)
        val band = dataset.findVariable("Band1")
        val data = band.read()
        val values = convertMa2Arrayto1DJavaArray(data)
        LOG.warn("Count of the point values are %d".format(values.count()))
        val sums = values.reduce(_+_)

        LOG.warn("Total summary of the point values are %s".format(sums))
/**
        val file = sc.textFile("<TEST_ASC_FILE>")
        val header = file.take(6)
        val data = file.filter(x => !header.contains(x))
        val values = data.map(x => x.split(","))
        val sums = values.map(x => x.map(_.toDouble).reduce(_+_))
        val total = sums.reduce(_+_)

        LOG.warn("Total summary of the point values are %s".format(total))
**/

         sc.stop()
    }

  def convertMa2Arrayto1DJavaArray(ma2Array: ma2.Array): Array[Double] = {
    ma2Array.getDataType match {
      case DataType.INT => ma2Array.copyTo1DJavaArray.asInstanceOf[Array[Int]].map(_.toDouble)
      case DataType.SHORT => ma2Array.copyTo1DJavaArray().asInstanceOf[Array[Short]].map(_.toDouble)
      case DataType.FLOAT => ma2Array.copyTo1DJavaArray.asInstanceOf[Array[Float]].map(_.toDouble)
      case DataType.DOUBLE => ma2Array.copyTo1DJavaArray.asInstanceOf[Array[Double]]
      case DataType.LONG => ma2Array.copyTo1DJavaArray().asInstanceOf[Array[Long]].map(_.toDouble)
      case badType => throw new Exception("Can't convert ma2.Array[" + badType + "] to numeric array.")
    }
  }


}

