package com.flipkart

import com.esri.core.geometry.{Envelope, Point}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by karan.verma on 01/05/17.
  */
object EnvelopeCheck {

  def contained_in_bangalore(long_cord: Double, lat_cord: Double) : Boolean =   {
    val bangalore = new Envelope(77.32537600000006, 12.658149000000037, 77.83579600000007, 13.234974000000022)
    val point = new Point(long_cord, lat_cord)
    bangalore.contains(point)
  }

}


//  def main(args: Array[String]) {
//    val conf = new SparkConf()
//    conf.setMaster("local[2]")
//    conf.setAppName("Accounts Data")
//    val sc = new SparkContext(conf)
//    // Long Lat Long Lat
//    val bangalore = new Envelope(77.32537600000006, 12.658149000000037, 77.83579600000007, 13.234974000000022);
//    val point_inside = new Point(77.7147144, 12.9566031);
//    val point_outise = new Point(75.2868824, 19.8829627);
//    new Point()
//    // Long Lat
//    println(bangalore.contains(point_inside))
//    print(bangalore.contains(point_outise))
//  }
