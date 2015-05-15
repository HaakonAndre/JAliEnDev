package alien.api.catalogue;

import java.util.List;

import alien.api.Request;
import alien.io.TransferDetails;

public class ListTransfer extends Request {
	List<TransferDetails> transfers;
	private String status;
	private String toSE;
	private String user;
	private String id;
	private boolean master;
	private boolean verbose;
	private boolean summary;
	private boolean all_status;
	private boolean jdl;
	private int count;
	
	public ListTransfer( String status,
			String toSE,
			String user,
			String id,
			boolean master,
			boolean verbose,
			boolean summary,
			boolean all_status,
			boolean jdl,
			int count ){
		this.status = status;
		this.toSE = toSE;
		this.user = user;
		this.id = id;
		this.master = master;
		this.verbose = verbose;
		this.summary = summary;
		this.all_status = all_status;
		this.jdl = jdl;
		this.count = count;
	}
	
	@Override
	public void run() {	}
	
	public List<TransferDetails> getTransfers(){
		return this.transfers;
	}
}
