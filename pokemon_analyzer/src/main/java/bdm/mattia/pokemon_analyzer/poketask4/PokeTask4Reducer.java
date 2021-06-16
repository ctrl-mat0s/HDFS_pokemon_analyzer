package bdm.mattia.pokemon_analyzer.poketask4;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PokeTask4Reducer extends Reducer<Text, Text, Text, Text>{

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException {
		
		String name = "";
		int highest = 0;
		
		Iterator<Text> valuesIt = values.iterator();
		while (valuesIt.hasNext()) {
			String str = valuesIt.next().toString();
			int stat = Integer.parseInt(str.split(",")[1]);
			if(stat>highest) {
				highest = stat;
				name = str.split(",")[0];
			}
		}
		String newValue = name + " - "+ highest; 
		context.write(key, new Text(newValue));
	}
}
