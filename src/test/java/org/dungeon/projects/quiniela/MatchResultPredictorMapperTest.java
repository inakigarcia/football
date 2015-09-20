package org.dungeon.projects.quiniela;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;import org.junit.Test; 

public class MatchResultPredictorMapperTest {   
    MapDriver<LongWritable, Text, Text, Text> mapDriver;  
    
    private static final String OSASUNA = "Osasuna";
    private static final String CELTA = "Celta";
    private static final String NUMANCIA = "Numancia";
    private static final String OVIEDO = "Oviedo";
    private static final String RAYO = "Rayo Vallecano";
    
    private static final String COMA = ",";
    private static final String SLASH = "-";
    
    private static final String MATCH_VISITOR_WINS = "1,00/01,1,1," + OSASUNA + COMA + CELTA + ",0,2";
    private static final String MATCH_HOST_WINS = "5,00/01,1,1," + NUMANCIA + COMA + OVIEDO + ",1,0";
    private static final String MATCH_DRAW = "22,00/01,1,3," + OSASUNA + COMA + RAYO + ",2,2";

    private static final String DRAW = "X";
    private static final String HOST = "1";
    private static final String VISITOR = "2";
  
    @Before  
    public void setUp() {    
        MatchResultPredictorMapper mapper = new MatchResultPredictorMapper();    
        mapDriver = MapDriver.newMapDriver(mapper);   
    }   

    @Test  
    public void when_visitor_has_higher_score_than_host_team_then_the_result_is_2_visitor_wins() throws IOException {    
        mapDriver.withInput(new LongWritable(), new Text(MATCH_VISITOR_WINS));
        mapDriver.withOutput(new Text(OSASUNA + SLASH + CELTA), new Text(VISITOR));    
        mapDriver.runTest();
    }    

    @Test  
    public void when_host_has_higher_score_than_visitor_team_then_the_result_is_1_host_wins() throws IOException {    
        mapDriver.withInput(new LongWritable(), new Text(MATCH_HOST_WINS));
        mapDriver.withOutput(new Text(NUMANCIA + SLASH + OVIEDO), new Text(HOST));    
        mapDriver.runTest();  
    }   

    @Test  
    public void when_host_and_visitor_has_the_same_score_then_the_result_is_X_draw() throws IOException {    
        mapDriver.withInput(new LongWritable(), new Text(MATCH_DRAW));
        mapDriver.withOutput(new Text(OSASUNA + SLASH + RAYO), new Text(DRAW));    
        mapDriver.runTest();  
    } 
}
