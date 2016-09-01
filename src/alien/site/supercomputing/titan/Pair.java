package alien.site.supercomputing.titan;

class Pair<F,S>{
	private F first;
	private S second;

	public Pair(F _first, S _second){
		first = _first;
		_second = _second;
	}

	public F getFirst(){
		return first;
	}
	public S getSecond(){
		return second;
	}
}
