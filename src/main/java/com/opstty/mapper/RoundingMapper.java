package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class RoundingMapper extends Mapper<Object, Text, Text, IntWritable> {

    private Text rounding = new Text();
    private String string;
    private static final IntWritable number = new IntWritable(1);
    private static final String DELIMITER = ";";


    public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {

        string = value.toString();
        String[] stringSplit = string.split(DELIMITER);

       // try{
            if(!stringSplit[1].equals("ARRONDISSEMENT")) {
                rounding.set(stringSplit[1]);
                context.write(rounding, number);
            }
        //}
        /*catch(IOException e){}
        catch(InterruptedException e2){}
        catch(NumberFormatException e3){}*/

    }

}
