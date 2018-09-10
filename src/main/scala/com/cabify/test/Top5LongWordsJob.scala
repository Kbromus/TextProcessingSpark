package com.cabify.test

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.rdd.RDD

object Top5LongWordsJob {
  
  def run(ssc:StreamingContext,file:String, rdd:RDD[String]){
    
    //seleccionamos las palabras de cada una de las lineas
    // y realizamos el conteo
    val words= Top5WordsJob.getWords(rdd)
    
    val wordCounts = words.map(x => (x, x.length())).distinct()
    
    //eliminamos keys vacias
    val filteredwords = wordCounts.filter(word => !word._1.isEmpty())
    
    //generamos un fichero con las 5 palabras que más aparecen
    ssc.sparkContext.parallelize(filteredwords.sortBy(x => -x._2).take(5), 1).saveAsTextFile("tests/output/"+file+"/top5longwords");
    
  }
  
}