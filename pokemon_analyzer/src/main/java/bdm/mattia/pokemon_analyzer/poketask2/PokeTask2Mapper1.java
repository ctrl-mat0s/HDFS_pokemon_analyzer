package bdm.mattia.pokemon_analyzer.poketask2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PokeTask2Mapper1 extends Mapper<LongWritable, Text, Text, IntWritable>{

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String row = value.toString();
		String[] column = row.split(",");
		String type = column[2];
		String gen = column[11];
		
		String newKey = gen + "," + type;
		
		context.write(new Text(newKey), new IntWritable(1));
	}
}
