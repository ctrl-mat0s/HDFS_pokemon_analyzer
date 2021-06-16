package bdm.mattia.pokemon_analyzer;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import bdm.mattia.pokemon_analyzer.poketask1.PokeTask1;
import bdm.mattia.pokemon_analyzer.poketask2.PokeTask2;
import bdm.mattia.pokemon_analyzer.poketask3.PokeTask3;
import bdm.mattia.pokemon_analyzer.poketask4.PokeTask4;

/********************************************
 *  Mattia Nardoni  -  Big Data Management	*
 *  										*
 *			   Pokemon Analyzer				*
 ********************************************/

public class PokemonAnalyzerMain extends Configured implements Tool{

	public static void main(String[] args) throws Exception{
		int exitCode = ToolRunner.run(new PokemonAnalyzerMain(), args);
		System.exit(exitCode);
	}

	public int run(String[] args) throws Exception {
		if (args.length != 3) {
			System.err.printf("Usage: %s [generic options] <input> <output>\n",
					getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		
		int exitCode = 0;
		int selectedTask = 0;
		selectedTask = Integer.parseInt(args[2]);
		
		switch (selectedTask) {
		case 1:
			System.out.println("Running the 1st PokeTask...");
			PokeTask pokeTask1 = new PokeTask1(args[0], args[1]);
			exitCode = pokeTask1.start();
			break;
		
		case 2:
			System.out.println("Running the 2nd PokeTask...");
			PokeTask pokeTask2 = new PokeTask2(args[0], args[1]);
			exitCode = pokeTask2.start();
			break;

		case 3:
			System.out.println("Running the 3rd PokeTask...");
			PokeTask pokeTask3 = new PokeTask3(args[0], args[1]);
			exitCode = pokeTask3.start();
			break;

		case 4:
			System.out.println("Running the 4th PokeTask...");
			PokeTask pokeTask4 = new PokeTask4(args[0], args[1]);
			exitCode = pokeTask4.start();
			break;

		default:
			break;
		}
		return exitCode;
	}
}
