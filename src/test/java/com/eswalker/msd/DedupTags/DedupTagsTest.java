/**
 * 
 */
package com.eswalker.msd.DedupTags;

import java.io.IOException;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import com.eswalker.msd.DedupTags.DedupTags.HMapper;


 
public class DedupTagsTest {
 
  MapDriver<LongWritable, Text, NullWritable, Text> mapDriver;
 
  @Before
  public void setUp() throws ParseException {

    HMapper mapper = new HMapper();
    mapDriver = MapDriver.newMapDriver(mapper);
  
  }
 
  @Test
  public void testMapper() throws IOException {
    mapDriver.withInput(new LongWritable(), new Text(
        "1|TRAID|artist|title|3|old,10|hello,100|HELLO,50"
    ));

    mapDriver.withOutput(NullWritable.get(), new Text("1|TRAID|artist|title|2|hello,100|old,10"));
    mapDriver.runTest();
  }
  
 
	  
 
}