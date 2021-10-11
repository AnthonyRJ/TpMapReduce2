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

import java.io.IOException;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class TreesByKindsMapperTest {

    @Mock
    private Mapper.Context context;
    private TreesByKindsMapper treesByKindsMapper;

    @Before
    public void setup() {
        this.treesByKindsMapper = new TreesByKindsMapper();
    }

    @Test
    public void testMap() throws IOException, InterruptedException {

        String value = "(48.8325900983, 2.41116455985);12;Platanus;x acerifolia;Platanaceae;1860;45.0;405.0;Ile de Bercy;Platane commun;;21;Bois de Vincennes (Ile de Bercy)";
        this.treesByKindsMapper.map(null, new Text(value), this.context);
        verify(this.context, times(1)).write(new Text("Platanus"), new IntWritable(1));
    }
}