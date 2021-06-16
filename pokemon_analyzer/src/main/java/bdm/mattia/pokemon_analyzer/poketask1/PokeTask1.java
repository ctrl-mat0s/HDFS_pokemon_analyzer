package bdm.mattia.pokemon_analyzer.poketask1;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import bdm.mattia.pokemon_analyzer.PokeTask;
import bdm.mattia.pokemon_analyzer.PokemonAnalyzerMain;

public class PokeTask1 extends PokeTask {

	private BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
	
	public PokeTask1(String arg0, String arg1) {
		super(arg0, arg1);
	}

	@Override
	protected int start() throws Exception {
		
		Configuration conf = new Configuration();
		System.out.print("Insert the GENERATION (from 1 to 6): ");
		String str = in.readLine();
		int gen = Integer.parseInt(str.split(" ")[0]);
		while(gen<1 || gen>6) {
			System.out.print("INVALID INPUT! Insert the GENERATION (from 1 to 6): ");
			str = in.readLine();
			gen = Integer.parseInt(str.split(" ")[0]);
		}
		conf.set("gen", str);
		
		Job job = Job.getInstance(conf,"PokeTask1");
		job.setJarByClass(PokemonAnalyzerMain.class);
		job.setJobName("PokeTask1");
		
		FileInputFormat.addInputPath(job, new Path(this.arg0));
		FileOutputFormat.setOutputPath(job, new Path(this.arg1));
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapperClass(PokeTask1Mapper.class);
		job.setReducerClass(PokeTask1Reducer.class);
	
		int returnValue = job.waitForCompletion(true) ? 0:1;
		System.out.println("job.isSuccessful " + job.isSuccessful());
		return returnValue;
	}

}
