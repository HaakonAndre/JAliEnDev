package alien.catalogue.recursive;

import alien.catalogue.LFN_CSD;

/**
 * @author mmmartin
 *
 */
public class Delete extends RecursiveOp {

	@Override
	public boolean callback(LFN_CSD lfnc) {
		return lfnc.exists;
	}

}
