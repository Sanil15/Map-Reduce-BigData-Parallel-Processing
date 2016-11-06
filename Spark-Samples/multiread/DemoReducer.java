package main;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import java.io.IOException;
import java.util.TreeMap;
import java.util.*;


class Player {

    public int year;
    public String name;
    public Double salary;
    public int homeRun;
    public String team;
    public Double per_homeRun;

    Player(){
        salary = -1.0;
        homeRun = -1;
    }
    
    @Override
    public String toString() {
        return "year:" + year + ", name:" + name + ", salary:" + salary + ", hr:" + homeRun + ", team:"
                + team + ", per_homerun:" + per_homeRun;
    }
    
} 


public class DemoReducer extends Reducer<Text, PlayerInfo, Text, Text> {
    
    TreeMap<Integer,Player> playerMap; 
    Player minPlayer;
    Double minHomeRun;

    public void setup(Context ctx){
        minPlayer = new Player();
        minHomeRun = Double.POSITIVE_INFINITY;
    }


    @Override
    protected void reduce(Text key, Iterable<PlayerInfo> vals, Context ctx) throws IOException, InterruptedException{
       
        playerMap = new TreeMap<Integer,Player>();
        //System.out.println(key.toString());
            
        String playerName = null;

        for(PlayerInfo xx: vals){
            if(xx.dataType() == 1){ 
                playerName = xx.getName();
                break;
            }
        }

        for (PlayerInfo xx : vals) {
            if(xx.dataType() == 2){
                 
                 if(playerMap.containsKey(xx.getYear())){
                     Player p = playerMap.get(xx.getYear());
                     p.name = playerName;
                     p.year = xx.getYear();
                     p.salary = xx.getSalary();
                     p.team = xx.getTeamId();
                     playerMap.put(xx.getYear(), p);
                 }
                 else{
                     Player p = new Player();
                     p.name = playerName;
                     p.year = xx.getYear();
                     p.salary = xx.getSalary();
                     p.team = xx.getTeamId();
                     playerMap.put(xx.getYear(), p);
                 }
             }
            else if(xx.dataType() == 3){
                
            
                 if(playerMap.containsKey(xx.getYear())){
                     Player p = playerMap.get(xx.getYear());
                     p.name = playerName;
                     p.year = xx.getYear();
                     p.homeRun = xx.getHomeRun();
                     p.team = xx.getTeamId();
                     playerMap.put(xx.getYear(), p);
                 }
                 else{
                     Player p = new Player();
                     p.name = playerName;
                     p.year = xx.getYear();
                     p.homeRun = xx.getHomeRun();
                     p.team = xx.getTeamId();
                     playerMap.put(xx.getYear(), p);
                 }
            }
                 
        }


        for(Map.Entry<Integer,Player> entry : playerMap.entrySet()){
            Player p = entry.getValue();

            if(p.salary > 0 && p.homeRun > 0){
            p.per_homeRun = (double) p.salary / p.homeRun;
            ctx.write(new Text(p.toString()),new Text(" "));

                if(p.per_homeRun.compareTo(minHomeRun) < 0){
                    minPlayer = p;
                    minHomeRun = p.per_homeRun;
                }
            }
        }




    }


    public void cleanup(Context ctx) throws IOException, InterruptedException{
        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        System.out.println("Lowest Player For Year "+minPlayer);
        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
    }
}
