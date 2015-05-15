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
			int count ){
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
	}
	
	@Override
	public void run(){
		this.transfers = new ArrayList<TransferDetails>();
		if( this.count==0 )
			return;
		List<TransferDetails> tlst;
		if(this.user==null || this.user.length()==0)
			tlst = TransferUtils.getActiveTransfersBySE(this.toSE);
		else
			tlst = TransferUtils.getActiveTransfersByUser(this.user);
		
		int found = 0;
		for( TransferDetails t : tlst ){
			if( this.count>0 && found==this.count )
				break;
			if( (this.user==null || this.user.length()==0 ) &&
					(this.toSE==null || this.toSE.length()==0) && 
					( !t.user.equals(this.user) || !t.destination.equals(this.toSE)))
				continue;
			if( (this.status!=null || this.status.length()!=0) 
					&& !t.status.equals(this.status))
				continue;
			if( (this.id!=null) 
					&& t.transferId!=this.id )
				continue;
			
			this.transfers.add(t);
			found++;
		}
	}
	
	public List<TransferDetails> getTransfers(){
		return this.transfers;
	}
}
