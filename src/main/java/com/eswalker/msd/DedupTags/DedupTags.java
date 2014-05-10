package com.eswalker.msd.DedupTags;

import java.io.IOException;
import java.util.HashMap;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class DedupTags extends Configured implements Tool{
	public static class HMapper extends Mapper<LongWritable, Text, NullWritable, Text> {	     
		
		private static NullWritable nullKey = NullWritable.get();
	    private static Text outputValue = new Text();
	   
	   
	    @Override
	    protected final void setup(final Context context) throws IOException, InterruptedException {
	  
	    }
	    
	    public class Tag implements Comparable<Tag>{
	    	public String tag;	public int score;
	    	public Tag(String tag, int score) { this.tag = tag; this.score = score;}
			public int compareTo(Tag that) { if (that.score == this.score) return this.tag.compareTo(that.tag); else return that.score - this.score; }
			public String toString() { return this.tag + "," + this.score; }
	    }

		@Override
		public final void map(final LongWritable key, final Text value, Context context) throws IOException, InterruptedException {
			try {
			
			String data[] = value.toString().split("\\|");
			
			HashMap<String, Integer> tagsToScore = new HashMap<String, Integer>();
			
			for(int i = 5; i < data.length; i++) {
				
				String data2[] = data[i].split(",");
				String tag = data2[0].toLowerCase();
				int score = Integer.parseInt(data2[1]);
				
				if (tagsToScore.containsKey(tag)) {
					if (tagsToScore.get(tag).intValue() < score)
						tagsToScore.put(tag, score);
				} else {
					tagsToScore.put(tag, score);
				}
			}
			
			TreeSet<Tag> tags = new TreeSet<Tag>();
			for (String tag : tagsToScore.keySet()) 
				tags.add(new Tag(tag, tagsToScore.get(tag)));
			
			StringBuilder sb = new StringBuilder();
			sb.append(data[0]);					sb.append('|');
			sb.append(data[1]);					sb.append('|');
			sb.append(data[2]);					sb.append('|');
			sb.append(data[3]);					sb.append('|');
			sb.append(tags.size()); 			sb.append('|');
			for (Tag tg : tags) {
				sb.append(tg.tag); 				sb.append(','); 
				sb.append(tg.score);			sb.append('|');
			}	
			sb.deleteCharAt(sb.length()-1);

			outputValue.set(sb.toString());
			context.write(nullKey, outputValue);
			
			} catch (Exception e) {return;}
			
		}
		
	
		
	}


    /**
     * Sets up job, input and output.
     * 
     * @param args
     *            inputPath outputPath
     * @throws Exception
     */
	public int run(String[] args) throws Exception {

       Configuration conf = getConf();
        
       Job job = new Job(conf);
       job.setJarByClass(DedupTags.class);
       job.setJobName("DedupTrackList");
       
       job.setInputFormatClass(TextInputFormat.class);
       
       TextInputFormat.addInputPaths(job, args[0]);
       TextOutputFormat.setOutputPath(job, new Path(args[1]));
       
       job.setNumReduceTasks(0);

       job.setMapperClass(HMapper.class);

       job.setMapOutputKeyClass(Text.class);
       job.setMapOutputValueClass(Text.class);
       job.setOutputKeyClass(NullWritable.class);
       job.setOutputValueClass(Text.class);

       return job.waitForCompletion(true) ? 0 : 1;
    }
	
    public static void main(String args[]) throws Exception {
    	
        if (args.length < 2) {
            System.out.println("Usage: DedupTrackList <input dirs> <output dir>");
            System.exit(-1);
        }
 
        int result = ToolRunner.run(new DedupTags(), args);
        System.exit(result);
        
   }


}

