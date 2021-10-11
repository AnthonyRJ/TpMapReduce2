package com.opstty.mapper;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TreesSmallToLargeMapper extends Mapper<Object, Text, Text, IntWritable>{

    private Text kind = new Text();
    private String string;
    private static final IntWritable number = new IntWritable(1);
    private DoubleWritable circumf = new DoubleWritable();
    private static final String DELIMITER = ";";

    public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {

        try{
            string = value.toString();
            String[] stringSplit = string.split(DELIMITER);

            kind.set(stringSplit[2]);
            circumf.set(Double.parseDouble(stringSplit[7]));

            context.write(circumf, kind);
        }
        catch(IOException e){}
        catch(InterruptedException e1){}
        catch(NumberFormatException e2){}

    }
}
