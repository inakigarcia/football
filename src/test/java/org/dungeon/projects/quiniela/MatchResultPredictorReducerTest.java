package org.dungeon.projects.quiniela;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List; 

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test; 

public class MatchResultPredictorReducerTest {   
    ReduceDriver<Text, Text, Text, Text> reduceDriver;  
    
    private static final Text MATCH = new Text("Osasuna-Celta");

    private static final String DRAW = "X";
    private static final String HOST = "1";
    private static final String VISITOR = "2";
  
    @Before  
    public void setUp() {    
        MatchResultPredictorReducer reducer = new MatchResultPredictorReducer();    
        reduceDriver = ReduceDriver.newReduceDriver(reducer);   
    } 
    
    @Test  
    public void when_the_most_frecuent_value_is_X_wich_means_draw_the_final_result_is_X() throws IOException {    
        List<Text> values = new ArrayList<Text>();    
        values.add(new Text(DRAW));    
        values.add(new Text(DRAW));    
        values.add(new Text(DRAW));    
        values.add(new Text(DRAW));    
        values.add(new Text(HOST));    
        values.add(new Text(HOST));    
        values.add(new Text(VISITOR));    
        reduceDriver.withInput(MATCH, values);    
        reduceDriver.withOutput(MATCH, new Text(DRAW));    
        reduceDriver.runTest();  
    }
    
    @Test  
    public void when_the_most_frecuent_value_is_2_wich_means_visitor_wins_the_final_result_is_2() throws IOException {    
        List<Text> values = new ArrayList<Text>();    
        values.add(new Text(VISITOR));    
        values.add(new Text(VISITOR));    
        values.add(new Text(VISITOR));    
        values.add(new Text(VISITOR));    
        values.add(new Text(HOST));    
        values.add(new Text(HOST));    
        values.add(new Text(DRAW));    
        reduceDriver.withInput(MATCH, values);    
        reduceDriver.withOutput(MATCH, new Text(VISITOR));    
        reduceDriver.runTest();  
    }
    
    @Test  
    public void when_the_most_frecuent_value_is_1_wich_means_host_wins_the_final_result_is_1() throws IOException {    
        List<Text> values = new ArrayList<Text>();    
        values.add(new Text(HOST));    
        values.add(new Text(HOST));    
        values.add(new Text(HOST));    
        values.add(new Text(HOST));    
        values.add(new Text(VISITOR));    
        values.add(new Text(VISITOR));    
        values.add(new Text(DRAW));    
        reduceDriver.withInput(MATCH, values);    
        reduceDriver.withOutput(MATCH, new Text(HOST));    
        reduceDriver.runTest();  
    }
    
    @Test  
    public void when_there_are_the_same_values_of_each_result_whe_choose_1_host_wins() throws IOException {    
        List<Text> values = new ArrayList<Text>();    
        values.add(new Text(HOST));      
        values.add(new Text(VISITOR));      
        values.add(new Text(DRAW));    
        reduceDriver.withInput(MATCH, values);    
        reduceDriver.withOutput(MATCH, new Text(HOST));    
        reduceDriver.runTest();  
    }
    
    @Test  
    public void when_the_less_frequent_value_is_1_and_are_same_X_and_2_then_we_choose_X() throws IOException {    
        List<Text> values = new ArrayList<Text>();    
        values.add(new Text(HOST));      
        values.add(new Text(VISITOR));      
        values.add(new Text(VISITOR));     
        values.add(new Text(DRAW));     
        values.add(new Text(DRAW));    
        reduceDriver.withInput(MATCH, values);    
        reduceDriver.withOutput(MATCH, new Text(DRAW));    
        reduceDriver.runTest();  
    }

}
