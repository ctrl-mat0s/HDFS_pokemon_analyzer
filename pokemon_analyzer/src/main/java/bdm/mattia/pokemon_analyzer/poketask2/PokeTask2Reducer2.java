package bdm.mattia.pokemon_analyzer.poketask2;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PokeTask2Reducer2 extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		int max = 0;
		String type = "";
		
		Iterator<Text> valuesIt = values.iterator();
		
		while (valuesIt.hasNext()) {
			String str = valuesIt.next().toString();
			int numPerType = Integer.parseInt(str.split(",")[1]);
			if(numPerType>max) {
				max = numPerType;
				type = str.split(",")[0];
			}
		}
		String newValue = type;
		context.write(key, new Text(newValue));
	}

}
