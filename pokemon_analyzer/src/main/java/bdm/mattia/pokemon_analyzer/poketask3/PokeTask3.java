package bdm.mattia.pokemon_analyzer.poketask3;

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
import bdm.mattia.pokemon_analyzer.poketask3.PokeTask3Mapper;
import bdm.mattia.pokemon_analyzer.poketask3.PokeTask3Reducer;

public class PokeTask3 extends PokeTask {

	private BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
	
	public PokeTask3(String arg0, String arg1) {
		super(arg0, arg1);
	}

	@Override
	protected int start() throws Exception {
		
		Configuration conf = new Configuration();
		
		//Verification of the input 
		System.out.print("Insert the TYPE (grass, fire, water, ...): ");
		String str1 = in.readLine();
		String type = str1.split(" ")[0];
		while(!(type.toLowerCase().equals("fire") || type.toLowerCase().equals("normal") || type.toLowerCase().equals("water") || 
				type.toLowerCase().equals("ground") || type.toLowerCase().equals("grass") || type.toLowerCase().equals("fairy") || 
				type.toLowerCase().equals("flying") || type.toLowerCase().equals("fighting") || type.toLowerCase().equals("ghost") ||
				type.toLowerCase().equals("psychic") || type.toLowerCase().equals("bug") || type.toLowerCase().equals("electric") || 
				type.toLowerCase().equals("dragon") || type.toLowerCase().equals("poison") || type.toLowerCase().equals("ice") || 
				type.toLowerCase().equals("rock") || type.toLowerCase().equals("steel"))) {
			System.out.print("INVALID INPUT! Insert the TYPE (grass, fire, water, ground, bug, electric, dragon, ice...): ");
			str1 = in.readLine();
			type = str1.split(" ")[0];
		}
		conf.set("type", type);
		
		System.out.print("Insert the STAT (HP, Attack, Defence, sp.Atk, sp.Def, Speed): ");
		String str2 = in.readLine();
		String stat = str2.split(" ")[0];
		while(!(stat.toLowerCase().equals("hp") || stat.toLowerCase().equals("attack") || stat.toLowerCase().equals("defence") || stat.toLowerCase().equals("sp.atk") || stat.toLowerCase().equals("sp.def") || stat.toLowerCase().equals("speed"))) {
			System.out.print("INVALID INPUT! Insert one of these STAT (HP, Attack, Defence, sp.Atk, sp.Def, Speed): ");
			str2 = in.readLine();
			stat = str2.split(" ")[0];
		}
		conf.set("stat", stat);
		
		Job job = Job.getInstance(conf,"PokeTask4");
		job.setJarByClass(PokemonAnalyzerMain.class);
		job.setJobName("PokeTask4");
		
		FileInputFormat.addInputPath(job, new Path(this.arg0));
		FileOutputFormat.setOutputPath(job, new Path(this.arg1));
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapperClass(PokeTask3Mapper.class);
		job.setReducerClass(PokeTask3Reducer.class);
	
		int returnValue = job.waitForCompletion(true) ? 0:1;
		System.out.println("job.isSuccessful " + job.isSuccessful());
		return returnValue;
	}

}
