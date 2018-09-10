package com.cabify.test

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.rdd.RDD

object Top5LongPhrasesJob {
  
  def run(ssc:StreamingContext,file:String, rdd:RDD[String]){
    

    val phrases = rdd.flatMap(_.split("[.?!]")).map(x => (x.trim(),x.trim().length())).distinct()
    
    //eliminamos keys vacias
    val filteredphrases = phrases.filter(word => !word._1.isEmpty())
    
    //generamos un fichero con las 5 palabras que más aparecen
    ssc.sparkContext.parallelize(filteredphrases.sortBy(x => -x._2).take(5), 1).saveAsTextFile("tests/output/"+file+"/top5phrases");
    
  }
  
}