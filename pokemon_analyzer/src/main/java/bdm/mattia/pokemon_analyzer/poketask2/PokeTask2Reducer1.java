package bdm.mattia.pokemon_analyzer.poketask2;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PokeTask2Reducer1 extends Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		
		int sum = 0;
		
		Iterator<IntWritable> valuesIt = values.iterator();
		while (valuesIt.hasNext()) {
			int tempVal =valuesIt.next().get();
			sum += tempVal;
		}
		context.write(key, new IntWritable(sum));
	}
}
