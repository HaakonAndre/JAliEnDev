package alien.site.supercomputing.titan;

public class Pair<F,S>{
	private F first;
	private S second;

	public Pair(F _first, S _second){
		first = _first;
		second = _second;
	}

	public F getFirst(){
		return first;
	}
	public S getSecond(){
		return second;
	}
}
