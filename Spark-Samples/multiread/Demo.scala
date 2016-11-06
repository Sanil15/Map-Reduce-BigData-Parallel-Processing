package main

import org.apache.hadoop.io._
import java.io.{ DataInput, DataOutput, IOException }
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.fs.Path
import org.apache.log4j.Logger
import org.apache.log4j.BasicConfigurator


object Demo {
    def main(args: Array[String]) {
        println("Demo: startup")

        // Configure log4j to actually log.
        BasicConfigurator.configure();

        // Make a job
        val job = Job.getInstance()
        job.setJarByClass(Demo.getClass)
        job.setJobName("Demo")

        // Set classes mapper, reducer, input, output.
        job.setReducerClass(classOf[DemoReducer])

        job.setMapOutputKeyClass(classOf[Text])
        job.setMapOutputValueClass(classOf[PlayerInfo])
        job.setOutputKeyClass(classOf[Text])
        job.setOutputValueClass(classOf[Text])
        

        // Set up number of mappers, reducers.
        job.setNumReduceTasks(2)

        MultipleInputs.addInputPath(job, new Path("baseball/Master.csv"), 
            classOf[TextInputFormat], classOf[NamesMapper])
        MultipleInputs.addInputPath(job, new Path("baseball/Salaries.csv"), 
            classOf[TextInputFormat], classOf[SalaryMapper])
        MultipleInputs.addInputPath(job, new Path("baseball/Batting.csv"), 
            classOf[TextInputFormat], classOf[BattingMapper])
        
        FileOutputFormat.setOutputPath(job, new Path("out"))

        // Actually run the thing.
        job.waitForCompletion(true)
    }
}


class PlayerInfo(var dataType: Int, var name: String, var year: Int, var salary: Double, var homeRun: Int, var teamId: String) extends Writable{


    def this() = this(0,"",0,0.0f,0,"")

    def getDataType() : Int = { 
        return dataType;
    }

    def getName() : String = { 
        return name
    }
    def getYear() : Int = {
        return year
    }
    def getSalary() : Double = { 
        return salary
    }
    def getHomeRun() : Int = { 
        return homeRun
    }
    def getTeamId() : String = {  
        return teamId
    }
    override def write(out: DataOutput) = {
    out.writeInt(dataType)
    out.writeUTF(name)
    out.writeInt(year)
    out.writeDouble(salary)
    out.writeInt(homeRun)
    out.writeUTF(teamId)
    }

    override def readFields(in: DataInput) = {
    dataType = in.readInt();
    name = in.readUTF();
    year = in.readInt();
    salary = in.readDouble();
    homeRun = in.readInt()
    teamId = in.readUTF();
    }
    

}


// Id : 1
class NamesMapper extends Mapper[Object, Text, Text, PlayerInfo] {
    type Context = Mapper[Object, Text, Text, PlayerInfo]#Context

    override def map(_k: Object, vv: Text, ctx: Context) {
        val cols = vv.toString.split(",")
        if (cols(0) == "playerID") {
            return
        }

        val playerID = cols(0)
        val name     = cols(15) + " " + cols(14)

        var player = new PlayerInfo(1,name,0,0.0f,0,"")

        ctx.write(new Text(playerID),player);
        
    }
}


// Id 2 var dataType: Int, var name: String, var year: Int, var salary: Double, var homeRun: Int, var teamId: String
class SalaryMapper extends Mapper[Object, Text, Text, PlayerInfo] {
    type Context = Mapper[Object, Text, Text, PlayerInfo]#Context

    override def map(_k: Object, vv: Text, ctx: Context) {
        val cols = vv.toString.split(",")
        if (cols(0) == "yearID") {
            return
        }

        val playerId = cols(3)
        val year = cols(0).toInt
        val salary = cols(4).toDouble
        val teamId = cols(1)

        var player = new PlayerInfo(2,"",year,salary,0,teamId)

       
        ctx.write(new Text(playerId),player)
        
    }
}

// Id 3 var dataType: Int, var name: String, var year: Int, var salary: Double, var homeRun: Int, var teamId: String
class BattingMapper extends Mapper[Object, Text, Text, PlayerInfo] {
    type Context = Mapper[Object, Text, Text, PlayerInfo]#Context

    override def map(_k: Object, vv: Text, ctx: Context) {
        val cols = vv.toString.split(",")
        if (cols(0) == "playerID") {
            return
        }

        val playerID = cols(0)
        val year = cols(1).toInt
        var homeRun = -1
        val teamId = cols(3)

        if(cols.length >= 11)
            homeRun = cols(11).toInt

        var player = new PlayerInfo(3,"",year,0.0d,homeRun,teamId)    

    
        ctx.write(new Text(playerID),player)
        
    }
}



// vim: set ts=4 sw=4 et:
