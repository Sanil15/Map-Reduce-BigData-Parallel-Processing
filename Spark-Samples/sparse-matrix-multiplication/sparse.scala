import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object sparse {
  
  val on_csv = """\s*,\s*""".r

    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("mmul").setMaster("local")
        val sc   = new SparkContext(conf)

        val textA = sc.textFile("_test/dataA.csv")
        val textB = sc.textFile("_test/dataB.csv")

         println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~SIZE OF RDDs~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        
        val rowsA = textA.map(line => on_csv.split(line));
        val rowsB = textB.map(line => on_csv.split(line));
        
        println("INPUT rowsA Count"+rowsA.count);
        
        println("INPUT rowsB Count"+rowsB.count);
        
        
        var countA = 999L;
        var countB = 999L;
        
       var dataA = rowsA.map { arr => (arr(0).toString().toLong,arr(1).toString().toLong,arr(2).toString().toDouble)}
                        .keyBy(row => (row._1,row._2))
                        .mapValues(row => row._3).persist();
                           
                           
       var dataB = rowsB.map { arr => (arr(0).toString().toLong,arr(1).toString().toLong,arr(2).toString().toDouble)}
                         .keyBy(row => (row._1,row._2))
                         .mapValues(row => row._3).persist();
        
        println("dataA Count"+dataA.count);
        
        println("dataB Count"+dataB.count);
   
        val nrangeA = (0 : Long) to (countA)
        val nrangeB = (0 : Long) to (countB)

        val distA = dataA.flatMap( cell => {
            val ((ii, kk), vv) = cell
            nrangeA.map(jj => ((ii, jj, kk), vv))
        })

        val distB = dataB.flatMap( cell => {
            val ((kk, jj), vv) = cell
            nrangeB.map(ii => ((ii, jj, kk), vv))
        })

        val distC = distA.join(distB).map( cell => {
            val ((ii, jj, kk), (aa, bb)) = cell
            ((ii, jj), aa * bb)
        })

        println("distA Count"+distA.count);
        
        println("distB Count"+distB.count);
        
        println("distC Count"+distC.count);
        
        val cellC = distC.reduceByKey( (xx, yy) => xx + yy )

        println("cellC Count"+cellC.count);
        
        val dataC = cellC.map( cell => {
            val ((ii, jj), vv) = cell
            (ii, (jj, vv))
        }).groupByKey.sortByKey(true).mapValues( xs => {
            xs.toArray.sortBy(_._1)
        }).map(row =>{    
              row._2.map( valu => {
                (row._1.toString()+","+valu._1.toString()+","+valu._2.toString())
              })
          
        }).flatMap(f => f);
        
        
        dataC.saveAsTextFile("output")
        
        println("cellC Count"+cellC.count);
        
        
        println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
       
        
        
        
        sc.stop()
        
        
    }
}