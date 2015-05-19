package alien.api.catalogue;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import alien.api.Request;
import alien.io.TransferDetails;
import alien.io.TransferUtils;
import alien.user.AliEnPrincipal;

public class ListTransfer extends Request {
	List<TransferDetails> transfers;
	private String status;
	private String toSE;
	private String user;
	private Integer id;
	private boolean master;
	private boolean verbose;
	private boolean summary;
	private boolean all_status;
	private boolean jdl;
	private int count;
	private boolean sort_desc;
	
	public ListTransfer(final AliEnPrincipal user, 
			final String role, 
			String toSE,
			String userTransfer,
			String status,
			Integer id,
			boolean master,
/*			boolean verbose,
			boolean summary, */
			//boolean all_status,
			//boolean jdl,
			int count,
			boolean desc){
		this.status = status;
		this.toSE = toSE;
		this.user = userTransfer;
		this.id = id;
		this.master = master;
/*		this.verbose = verbose;
		this.summary = summary; */
//		this.all_status = all_status;
//		this.jdl = jdl;
		this.count = count;
		this.sort_desc = desc;
	}
	
	@Override
	public void run(){
		this.transfers = new ArrayList<TransferDetails>();
		if( this.count==0 )
			return;
		List<TransferDetails> tlst;
		
		tlst = TransferUtils.getAllActiveTransfers( this.toSE,
													this.user,
													this.status,
													this.id,
													(this.count==-1 ? null : this.count),
													this.sort_desc
					);
		
		for( TransferDetails t : tlst ){			
			this.transfers.add(t);		
		}
	}
	
	public List<TransferDetails> getTransfers(){
		return this.transfers;
	}
}
