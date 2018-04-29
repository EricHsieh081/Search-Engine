

import java.io.File
import java.io.FileInputStream
import java.io.InputStream
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.util.matching._

object PageRank {

  def main(args:Array[String]) {
    val filePath = "hdfs://Quanta006:8100/opt/Assignment3/Input/" + args(0)
    val outputPath = "hdfs://Quanta006:8100/user/100062243/HW3/pagerank_table10/"
    
    val conf = new SparkConf().setAppName("PageRank")
                              .set("spark.executor.memory", "24g")
                              .set("spark.driver.maxResultSize", "0")
                              .set("spark.cores.max", "50")
    val sc = new SparkContext(conf)
    val lines = sc.textFile(filePath)
    
    val src = "<title>.+</title>".r
    val out = "\\[\\[[^\\]]*(\\][^\\]]+)*\\]\\]".r
    
    val data = lines.map{x => 
      val str = (src findFirstIn x).toString()
      val title = str.substring(str.indexOf(">") + 1, str.lastIndexOf("<"))
      val temp = x.substring(x.indexOf(">", x.indexOf("<text")) + 1, x.indexOf("</text"))
      val links = (out findAllIn temp).map{x => x.substring(2, x.length()-2)}.toList //use reg to help
      (title, links)
    }// initial table
     
    val dataInverted = data.flatMap{x =>
        x._2.map { y =>  
          (y, x._1)  
        }
    }.groupByKey().mapValues { x => x.toList } //inverted the initial table
    
   val dataJoinInverted = data.join(dataInverted).map{x =>
     (x._1, x._2._2)
   } //join the two tables
   
   val dataJoin = dataJoinInverted.flatMap{x =>
      x._2.map { y =>
        (y, x._1)  
      }
   }.groupByKey().mapValues {x => x.toList} //inverted the joined table
   
   val dataNew = dataJoin ++ data.subtractByKey(dataJoin).mapValues { x => List.empty[String] }
   val dataFurther = dataNew.map{x =>
     (x._1, (1.0, x._2))
   }.cache()
   // val dataFrom = data.map{x => x._1}.toArray()
   // val dataFurther = data.map{x => // remove the links without in title
   //   val tag = x._2._2.filter {y => dataFrom.contains(y)}
   //   (x._1, (x._2._1, tag))
   // }
    
    val n = dataFurther.count()
    val iter = 10
    println(n)
    
    var rank = dataFurther.distinct()
    var arr = Seq[Double]()
    for(i<-1 to iter) {
      var elementA = rank.flatMap{x => //dandling will disappear because of no out link
        val outLinkNum = x._2._2.size
        x._2._2.map{y => 
          (y, x._2._1/outLinkNum)
        } 
      }.reduceByKey(_+_) //same src will be grouped
      
      // after join (page, (newRank, (oldRank, tag)))
      var withoutDangling = elementA.join(dataFurther).map(x => (x._1, (x._2._1*0.85, x._2._2._2)))
    
      var elementB = rank.filter(x => x._2._2.size == 0).map(x => x._2._1).sum() //find dangling
      var withDangling = rank.map{x =>  //reconstruct a new list
        (x._1, (elementB*0.85/n+0.15, x._2._2))
      }
      
      var temp = rank;
      rank = withoutDangling.union(withDangling).reduceByKey((x,y) => (x._1 + y._1, y._2)) //List use ++
     
      arr = arr ++ Seq(temp.join(rank).map{x => Math.abs(x._2._2._1 - x._2._1._1)}.sum())
    }
    
    val ss = rank.mapValues{ case(rank, link) => rank }.sortBy(_._2, false, 32)
    ss.saveAsTextFile(outputPath)
//    
//    sc.parallelize(
//      rank.map{case(page, (rank, link)) => (page, rank)}
//        .collect().sortBy{case(page, rank) => (rank, page)}(Ordering[(Double, String)].reverse)
//      , 10
//    ).saveAsTextFile(outputPath)
    
    println("total : " + rank.map(x => x._2._1).sum())
    arr.foreach { x => println(x) }
  }
}

