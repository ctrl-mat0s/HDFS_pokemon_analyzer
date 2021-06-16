package bdm.mattia.pokemon_analyzer.poketask2;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PokeTask2Mapper2 extends Mapper<Text, Text, Text, Text> {

	@Override
	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		String line = key.toString();
		String gen = line.split(",")[0];
		String type = line.split(",")[1];
		String numb = value.toString();
		
		String newKey = gen;
		String newValue = type + "," + numb;
		
		context.write(new Text(newKey), new Text(newValue));
	}

}
