package bdm.mattia.pokemon_analyzer.poketask3;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PokeTask3Mapper extends Mapper<LongWritable, Text, Text, Text>{

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		String pokemonType = conf.get("type");
		String pokemonStat = conf.get("stat");
		String row = value.toString();
		String[] column = row.split(",");
		String name = column[1];
		String type = column[2];
		String stat = "";
		
		if(pokemonStat.toLowerCase().equals("hp"))
			stat = column[5];
		else if(pokemonStat.toLowerCase().equals("attack"))
			stat = column[6];
		else if(pokemonStat.toLowerCase().equals("defence"))
			stat = column[7];
		else if(pokemonStat.toLowerCase().equals("sp.atk"))
			stat = column[8];
		else if(pokemonStat.toLowerCase().equals("sp.def"))
			stat = column[9];
		else if(pokemonStat.toLowerCase().equals("speed"))
			stat = column[10];
		
		String newValue = name + "," + stat;
		
		
		if(!type.equals("") && type.toLowerCase().equals(pokemonType.toLowerCase()))
			context.write(new Text(type), new Text(newValue));
	}

}
