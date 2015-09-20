package org.dungeon.projects.quiniela;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Test; 

public class MatchResultPredictorDriverTest {   
    
    
    String[] matches = new String[]{"1,00/01,1,1,Osasuna,Celta,0,2", "1,00/01,1,1,Osasuna,Celta,0,1", "1,00/01,1,1,Osasuna,Celta,0,3", 
                                    "5,00/01,1,1,Numancia,Oviedo,1,0", "5,00/01,1,1,Numancia,Oviedo,2,0", "5,00/01,1,1,Numancia,Oviedo,0,0"};   
    
    @Test
    public void integrate_results_for_the_same_match() throws IOException{

        new MapReduceDriver<LongWritable, Text, Text, Text, Text, Text>()
                .withMapper(new MatchResultPredictorMapper())
                .withInput(new LongWritable(1),new Text(matches[0]))
                .withInput(new LongWritable(2),new Text(matches[1]))
                .withInput(new LongWritable(3),new Text(matches[2]))
                .withReducer(new MatchResultPredictorReducer())
                .withOutput(new Text("Osasuna-Celta"),new Text("2"))
                .runTest();
    }
    
    @Test
    public void integrate_results_for_two_matches() throws IOException{

        new MapReduceDriver<LongWritable, Text, Text, Text, Text, Text>()
                .withMapper(new MatchResultPredictorMapper())
                .withInput(new LongWritable(1),new Text(matches[0]))
                .withInput(new LongWritable(2),new Text(matches[1]))
                .withInput(new LongWritable(3),new Text(matches[2]))
                .withInput(new LongWritable(4),new Text(matches[3]))
                .withInput(new LongWritable(5),new Text(matches[4]))
                .withInput(new LongWritable(6),new Text(matches[5]))
                .withReducer(new MatchResultPredictorReducer())
                .withOutput(new Text("Numancia-Oviedo"),new Text("1"))
                .withOutput(new Text("Osasuna-Celta"),new Text("2"))
                .runTest();
    }

}