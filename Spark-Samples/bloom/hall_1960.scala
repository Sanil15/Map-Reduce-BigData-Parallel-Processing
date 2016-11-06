
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import java.lang._

object hall1960 {
    def main(args: Array[String]) {
       val conf = new SparkConf().
            setAppName("Hall of Fame").
            setMaster("local")
        val sc = new SparkContext(conf)
       
        // Input
        val players = sc.textFile("baseball/Master.csv").
            map { _.split(",") }.
            filter { _(0) != "playerID"}.
            keyBy { _(0) }

        var salary = sc.textFile("baseball/Salaries.csv").
            map {_.split(",")}.
            filter ( row => (row(3) != "playerID" && row.length == 5 )).
            map {row => ( (row(3),row(0),row(1)), (row(4).toDouble))}.
            keyBy {a=>(a._1)}    

        val batting = sc.textFile("baseball/Batting.csv").
            map { _.split(",") }.
            filter (row => (row(0) != "playerID" && row.length > 11)).
            map {row => ( (row(0),row(1),row(3)), (row(11).toDouble))}.
            keyBy {a=>(a._1)}

    
        val data = batting.join(salary)

        val filter = data.filter { row => ( (row._2._1._2 != 0) && (row._2._2._2 != 0 ) )}

        var list = filter.map( row => ( (row._1), ((row._2._2._2)/(row._2._1._2)), (row._2._1), (row._2._2)))

        var sortList = list.sortBy(row => row._2);

        var top5 = sc.parallelize(sortList.take(5));

        var lowest = top5.keyBy(row => row._1._1)

        var output = lowest.join(players);

        output.saveAsTextFile("output")

        // val text = data.mapValues(row => {
        
        // val columns1 = row._1.toSeq.toList
        // val salary = columns1(4).toDouble
        
        // val columns2 = row._2.toSeq.toList
        
        // var homeRun = 0.0
        // if(columns2.length >=11)
        //    homeRun = columns2(11).toDouble
        // var perHomeRun = Double.MAX_VALUE
        //     if(homeRun!=0)
        //       perHomeRun = salary / homeRun
        
        // perHomeRun+","+columns1.mkString(",") +"," + columns2.mkString(",")
        
        // })

            
        // val sorted = text.sortBy(line => line._2);    
          
        // sorted.saveAsTextFile("output")

        // Shut down Spark, avoid errors.
        sc.stop()
    }
}


// vim: set ts=4 sw=4 et:
