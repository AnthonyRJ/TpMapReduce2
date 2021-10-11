package com.opstty.reducer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TreesSmallToLargeReducer extends Reducer<DoubleWritable, Text, DoubleWritable, Text>{

    public void reduce(DoubleWritable key, Iterable<Text> values, Reducer.Context context) throws IOException, InterruptedException {

        try{
            for(Text kind : values){
                context.write(key, kind);
            }
        }
        catch(IOException e){}
        catch(InterruptedException e1){}
    }
}
