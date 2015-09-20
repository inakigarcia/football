package org.dungeon.projects.quiniela;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MatchResultPredictorReducer extends Reducer<Text, Text, Text, Text> {

    private static final String DRAW = "X";
    private static final String HOST = "1";
    private static final String VISITOR = "2";

    private Text valueText = new Text();

    @Override    
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    	valueText.set(getMostFrequentResult(values));
		context.write(key, valueText);
    }
    
    private String getMostFrequentResult(Iterable<Text> results) {
        int count1 = 0;
        int countX = 0;
        int count2 = 0;
        String result = DRAW;
        for(Text text:results) {
            result = text.toString();
            if(HOST.equals(result)) {
                count1++;
            }
            if(DRAW.equals(result)) {
                countX++;
            }
            if(VISITOR.equals(result)) {
                count2++;
            }
        }
    
        int max = Math.max(count1, countX);
        max = Math.max(count2, max);
    
        result = VISITOR;
        if(max == countX) {
            result = DRAW;
        }
        if(max == count1) {
            result = HOST;
        }
    
        return result;
    }
}

    
//(Betis-Sevilla, [1,1,X,X,X,X,2,2]) -> (Betis-Sevilla, X)