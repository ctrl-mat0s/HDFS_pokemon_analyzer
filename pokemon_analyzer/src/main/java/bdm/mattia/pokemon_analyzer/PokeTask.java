package bdm.mattia.pokemon_analyzer;

public abstract class PokeTask {

	protected String arg0;
	protected String arg1;
	
	public PokeTask(String arg0, String arg1) {
		this.arg0 = arg0;
		this.arg1 = arg1;
	}
	
	protected abstract int start() throws Exception;
	
}
