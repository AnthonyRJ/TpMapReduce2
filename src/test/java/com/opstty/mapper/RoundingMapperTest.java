package com.opstty.mapper;

import junit.framework.TestCase;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class RoundingMapperTest extends TestCase {

    @Mock
    private Mapper.Context context;
    private RoundingMapper roundingMapper;

    @Before
    public void setup() {
        this.roundingMapper = new RoundingMapper();
    }

    @Test
    public void testMap() throws IOException, InterruptedException {
        String value = "a;2;7;aba;10;abababa";
        this.roundingMapper.map(null, new Text(value), this.context);
        verify(this.context, times(1)).write(new Text("2"), new IntWritable(1));
    }
}