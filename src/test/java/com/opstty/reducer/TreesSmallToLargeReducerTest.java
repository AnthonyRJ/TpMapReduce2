package com.opstty.reducer;

import junit.framework.TestCase;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.Arrays;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class TreesSmallToLargeReducerTest extends TestCase {
    @Mock
    private Reducer.Context context;
    private TreesSmallToLargeReducer treesSmallToLargeReducer;

    @Before
    public void setup() {
        this.treesSmallToLargeReducer = new TreesSmallToLargeReducer();
    }

    @Test
    public void testReduce() throws IOException, InterruptedException {
        DoubleWritable key = new DoubleWritable(140.5);
        DoubleWritable key2 = new DoubleWritable(47.6);
        DoubleWritable key3= new DoubleWritable(94.5);

        Text value1 = new Text("Platanus");
        Text value2 = new Text("Acer");
        Text value3 = new Text("Alnus");
        Text value4 = new Text("Fraxinus");

        Iterable<Text> values = Arrays.asList(value1, value2, value3,value4);

        this.treesSmallToLargeReducer.reduce(key, values, this.context);
        this.treesSmallToLargeReducer.reduce(key2, values, this.context);
        this.treesSmallToLargeReducer.reduce(key3, values, this.context);

        verify(this.context,times(1)).write(key, new Text("Platanus"));
        verify(this.context,times(1)).write(key2, new Text("Platanus"));
        verify(this.context,times(1)).write(key3, new Text("Platanus"));
        System.out.println(this.context);
    }
}