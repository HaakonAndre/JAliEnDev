package alien.catalogue.recursive;

import java.util.Set;
import java.util.TreeSet;

import alien.catalogue.LFN_CSD;

/**
 * @author mmmartin
 *
 */
public abstract class RecursiveOp {
	final Set<LFN_CSD> lfns_ok = new TreeSet<>();
	final Set<LFN_CSD> lfns_error = new TreeSet<>();
	boolean recurse_infinitely = false;
	boolean onlyAppend = false;

	/**
	 * @param lfnc
	 * @return true if no problem and recursion can continue
	 */
	public abstract boolean callback(LFN_CSD lfnc);

	/**
	 * @return recurse_infinitely
	 */
	public boolean getRecurseInfinitely() {
		return recurse_infinitely;
	}

	/**
	 * @param ri
	 */
	public void setRecurseInfinitely(final boolean ri) {
		this.recurse_infinitely = ri;
	}

	/**
	 * @return onlyAppend
	 */
	public boolean getOnlyAppend() {
		return onlyAppend;
	}

	/**
	 * @param oa
	 */
	public void setOnlyAppend(final boolean oa) {
		this.onlyAppend = oa;
	}

	/**
	 * @return lfns_ok
	 */
	public Set<LFN_CSD> getLfnsOk() {
		return lfns_ok;
	}

	/**
	 * @return lfns_error
	 */
	public Set<LFN_CSD> getLfnsError() {
		return lfns_error;
	}

}
