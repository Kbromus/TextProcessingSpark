package com.cabify.test

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.rdd.RDD
import scala.util.matching.Regex

object Top5WordsJob {
  
  
  def run(ssc:StreamingContext,file:String, rdd:RDD[String]){
    
    //seleccionamos las palabras de cada una de las lineas
    // y realizamos el conteo
    val wordCounts = getWords(rdd).map(x => (x, 1)).reduceByKey(_ + _)
    
    //eliminamos keys vacias
    val filteredwords = wordCounts.filter(word => !word._1.isEmpty())
    
    //generamos un fichero con las 5 palabras que más aparecen
    ssc.sparkContext.parallelize(filteredwords.sortBy(x => -x._2).take(5), 1).saveAsTextFile("tests/output/"+file+"/top5words");
    
  }
  
  def getWords(lines:RDD[String]): RDD[String] = {
    val words = lines.flatMap(_.split(" ")).map(word =>{
      
      val pattern = new Regex("\\W")
      var result=pattern.replaceAllIn(word, "").trim()
      result
    })
    words
  }
  
  
}