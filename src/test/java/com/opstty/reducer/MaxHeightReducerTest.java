package com.opstty.reducer;

import junit.framework.TestCase;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.Arrays;

import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class MaxHeightReducerTest extends TestCase {

    @Mock
    private Reducer.Context context;
    private MaxHeightReducer maxHeightReducer;

    @Before
    public void setup() {
        this.maxHeightReducer = new MaxHeightReducer();
    }

    @Test
    public void testReduce() throws IOException, InterruptedException {
        String key = "Platanus";

        DoubleWritable value1 = new DoubleWritable(14.0);
        DoubleWritable value2 = new DoubleWritable(17.0);
        DoubleWritable value3 = new DoubleWritable(9.0);
        DoubleWritable value4 = new DoubleWritable(31.0);
        DoubleWritable value5 = new DoubleWritable(9.3);

        Iterable<DoubleWritable> values = Arrays.asList(value1, value2, value3,value4,value5);

        this.maxHeightReducer.reduce(new Text(key), values, this.context);
        verify(this.context).write(new Text(key), new DoubleWritable(31.0));
    }
}