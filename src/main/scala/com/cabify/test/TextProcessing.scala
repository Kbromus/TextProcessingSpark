package com.cabify.test

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.io.LongWritable
import org.apache.spark.rdd.RDD

object TextProcessing {
  
      def main(args: Array[String]) {
        if (args.length < 1) {
          System.err.println("Usage: TextProcessing <directory>")
          System.exit(1)
        }
    
        val sparkConf = new SparkConf().setAppName("TextProcessing")
        // Creamos el streaming context
        val ssc = new StreamingContext(sparkConf, Seconds(60))
    
        // Creamos el textFileStream
        val stream = ssc.textFileStream(args(0))
        

        object GetFileNameFromStream extends java.io.Serializable { def getFileName(file: RDD[String]) :String ={ file.id+"" } }

        stream.foreachRDD(dataRDD => { 
          
          var ltotales=dataRDD.count()
          //comprobamos que hay datos para sacar ficheros y estadisticas
          if(ltotales > 0){

          //obtenemos el filename del stream
          val file= GetFileNameFromStream.getFileName(dataRDD)
          //filtramos las lineas que no sean de anotaciones
          val linestxt = dataRDD.filter(line => !line.contains("<") && !line.contains(">"))
          
          /**
           * Ejecutamos cada uno de los jobs para obtener los rankings solicitados
           */        
          
            Top5WordsJob.run(ssc,file,linestxt)
            Top5LongWordsJob.run(ssc, file, linestxt)
            Top5LongPhrasesJob.run(ssc, file, linestxt)
            
            
            var texto = StringBuilder.newBuilder
            
            var ltexto=linestxt.count()
            var porctexto = (ltexto.toFloat/ltotales)*100
            
            var palabrastotales= Top5WordsJob.getWords(linestxt).count()
            var palabrasunicas= Top5WordsJob.getWords(linestxt).distinct().count()
            
            
            texto.append("Estadisticas del proceso:\n lineas totales: "+ltotales+"\n lineas de texto: "+ltexto+
                "\n palabras totales: "+palabrastotales+"\n palabras unicas: "+palabrasunicas+"\n porcentaje de texto en fichero: "+porctexto)
                
            ssc.sparkContext.parallelize(texto.toString().split("\n").toSeq , 1).saveAsTextFile("tests/output/"+file+"/estadisticas");
          }
             
        })
          
        ssc.start()
        ssc.awaitTermination()
      }
      
}
