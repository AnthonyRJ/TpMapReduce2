package com.opstty.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class RoundingReducer extends Reducer<Text, IntWritable, IntWritable, IntWritable>{
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Reducer.Context context) throws IOException, InterruptedException {

        try{
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            result.set(sum);

            IntWritable parsedKey = new IntWritable(Integer.parseInt(key.toString()));
            context.write(parsedKey, result);
        }
        catch(IOException e){}
        catch(InterruptedException e1){}

    }
}
