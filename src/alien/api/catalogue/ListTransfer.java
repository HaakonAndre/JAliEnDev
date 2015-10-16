package alien.api.catalogue;

import java.util.ArrayList;
import java.util.List;

import alien.api.Request;
import alien.io.TransferDetails;
import alien.io.TransferUtils;
import alien.user.AliEnPrincipal;

/**
 * List transfers
 */
public class ListTransfer extends Request {
	/**
	 * 
	 */
	private static final long serialVersionUID = -4583213128245636923L;
	List<TransferDetails> transfers;
	private final String status;
	private final String toSE;
	private final String user;
	private final Integer id;
	private final int count;
	private final boolean sort_desc;

	/**
	 * @param user
	 * @param role
	 * @param toSE
	 * @param userTransfer
	 * @param status
	 * @param id
	 * @param master
	 * @param count
	 * @param desc
	 */
	public ListTransfer(final AliEnPrincipal user, final String role, final String toSE, final String userTransfer, final String status, final Integer id, final boolean master,
	/*
	 * boolean verbose, boolean summary,
	 */
	// boolean all_status,
	// boolean jdl,
			final int count, final boolean desc) {
		this.status = status;
		this.toSE = toSE;
		this.user = userTransfer;
		this.id = id;
		/*
		 * this.verbose = verbose; this.summary = summary;
		 */
		// this.all_status = all_status;
		// this.jdl = jdl;
		this.count = count;
		this.sort_desc = desc;
	}

	@Override
	public void run() {
		this.transfers = new ArrayList<>();
		if (this.count == 0)
			return;
		List<TransferDetails> tlst;

		tlst = TransferUtils.getAllActiveTransfers(this.toSE, this.user, this.status, this.id, (this.count == -1 ? null : Integer.valueOf(this.count)), this.sort_desc);

		for (final TransferDetails t : tlst)
			this.transfers.add(t);
	}

	/**
	 * @return transfer list
	 */
	public List<TransferDetails> getTransfers() {
		return this.transfers;
	}
}
