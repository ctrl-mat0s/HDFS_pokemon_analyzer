package bdm.mattia.pokemon_analyzer.poketask1;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PokeTask1Reducer extends Reducer<Text, Text, Text, Text>{

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		Iterator<Text> valuesIt = values.iterator();
		String str = valuesIt.next().toString();

		context.write(key, new Text(str));
	}		
}

