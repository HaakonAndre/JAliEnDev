package alien.catalogue.recursive;

import java.util.Set;
import java.util.TreeSet;

import alien.catalogue.LFN_CSD;
import alien.user.AliEnPrincipal;

/**
 * @author mmmartin
 *
 */
public abstract class RecursiveOp {
	final Set<LFN_CSD> lfns_ok = new TreeSet<>();
	final Set<LFN_CSD> lfns_error = new TreeSet<>();
	boolean recurse_infinitely = false;
	boolean onlyAppend = false;
	AliEnPrincipal user = null;
	LFN_CSD lfnc_target = null;
	LFN_CSD lfnc_target_parent = null;

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
	 * @return user
	 */
	public AliEnPrincipal getuser() {
		return user;
	}

	/**
	 * @param us
	 */
	public void setUser(final AliEnPrincipal us) {
		this.user = us;
	}

	/**
	 * @return lfn_target
	 */
	public LFN_CSD getLfnTarget() {
		return lfnc_target;
	}

	/**
	 * @param lfnct
	 */
	public void setLfnTarget(final LFN_CSD lfnct) {
		this.lfnc_target = lfnct;
	}

	/**
	 * @return user
	 */
	public LFN_CSD getLfnTargetParent() {
		return lfnc_target_parent;
	}

	/**
	 * @param lfnctp
	 */
	public void setLfnTargetParent(final LFN_CSD lfnctp) {
		this.lfnc_target_parent = lfnctp;
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
