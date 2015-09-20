package org.dungeon.projects.quiniela;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MatchResultPredictorMapper extends Mapper<LongWritable, Text, Text, Text> { 
    
    private static final String KEY_SEPARATOR = "-";
    private static final String TOKEN_SEPARATOR = ",";
    private static final String DRAW = "X";
    private static final String HOST = "1";
    private static final String VISITOR = "2";
    
    private Text keyText = new Text();
    private Text valueText = new Text();
       
    @Override    
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(TOKEN_SEPARATOR);
        String matchResult = calculateResult(tokens[6], tokens[7]); 
        StringBuffer matcKey = new StringBuffer(100);
        matcKey.append(tokens[4]).append(KEY_SEPARATOR).append(tokens[5]);     
        
        keyText.set(matcKey.toString());
        valueText.set(matchResult);
        context.write(keyText, valueText);
    }
    
    private String calculateResult(String hostScore, String visitorScore) {
        String result = DRAW;
        if(Integer.valueOf(hostScore) > Integer.valueOf(visitorScore)) {
            result = HOST;
        }
        if(Integer.valueOf(hostScore) < Integer.valueOf(visitorScore)) {
            result = VISITOR;
        }
        return result;
    }

}

//(Betis-Sevilla, [1,1,x,x,x,x,2,2])