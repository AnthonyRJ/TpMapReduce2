package com.opstty.mapper;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MaxHeightMapper extends Mapper<Object, Text, Text, DoubleWritable>{

    private String string;
    private Text kind = new Text();
    private DoubleWritable height = new DoubleWritable();
    private static final String DELIMITER = ";";


    public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {

        try{
            string = value.toString();
            String[] stringSplit = string.split(DELIMITER);

            if(!stringSplit[6].equals("HAUTEUR")) {
                kind.set(stringSplit[2]);
                height.set(Double.parseDouble(stringSplit[6]));
                context.write(kind, height);
            }
        }catch(NumberFormatException e){}

    }
}
