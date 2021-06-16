package bdm.mattia.pokemon_analyzer.poketask1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PokeTask1Mapper extends Mapper<LongWritable, Text, Text, Text>{

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		String pokemonGen = conf.get("gen");
		String row = value.toString();
		String[] column = row.split(",");
		String name = column[1];
		String gen = column[11];
		
		if(!gen.equals("") && gen.equals(pokemonGen))
			context.write(new Text(name), new Text(gen));
	}

}
