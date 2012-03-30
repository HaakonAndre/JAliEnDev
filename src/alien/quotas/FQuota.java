package alien.quotas;

import java.io.Serializable;

import lazyj.DBFunctions;
import lazyj.StringFactory;

/**
 * @author costing
 * @since 2012-03-30
 */
public final class FQuota implements Serializable, Comparable<FQuota> {

	/*
user                  | varchar(64)
totalSize             | bigint(20) 
maxNbFiles            | int(11)    
nbFiles               | int(11)    
tmpIncreasedTotalSize | bigint(20) 
maxTotalSize          | bigint(20) 
tmpIncreasedNbFiles   | int(11)    
	 */

	/**
	 * 
	 */
	private static final long serialVersionUID = 7587668615003121402L;

	/**
	 * AliEn account name
	 */
	public final String user;
	
	/**
	 * Total size of the stored files
	 */
	public final long	totalSize;
	
	/**
	 * Max number of files allowed
	 */
	public final int	maxNbFiles;
	
	/**
	 * Current number of files stored by this user
	 */
	public final int	nbFiles;
	
	/**
	 * Temp increase
	 */
	public final long	tmpIncreasedTotalSize;
	
	/**
	 * Max allowed total size of this users' files
	 */
	public final long	maxTotalSize;
	
	/**
	 * Temp increase
	 */
	public final int	tmpIncreasedNbFiles;
	
	/**
	 * @param db
	 */
	FQuota(final DBFunctions db){
		this.user = StringFactory.get(db.gets("user").toLowerCase());
		this.totalSize = db.getl("totalSize");
		this.maxNbFiles = db.geti("maxNbFiles");
		this.nbFiles = db.geti("nbFiles");
		this.tmpIncreasedTotalSize = db.getl("tmpIncreasedTotalSize");
		this.maxTotalSize = db.getl("maxTotalSize");
		this.tmpIncreasedNbFiles = db.geti("tmpIncreasedNbFiles");
	}

	@Override
	public int compareTo(final FQuota o) {
		return this.user.compareTo(o.user);
	}
	
	@Override
	public String toString() {
		return "FQuota: user: "+user+"\n"+
		       "totalSize\t: "+totalSize+"\n"+
		       "maxNbFiles\t: "+maxNbFiles+"\n"+
		       "nbFiles\t: "+nbFiles+"\n"+
		       "tmpIncreasedTotalSize\t: "+tmpIncreasedTotalSize+"\n"+
		       "maxTotalSize\t: "+maxTotalSize+"\n"+
		       "tmpIncreasedNbFiles\t: "+tmpIncreasedNbFiles;
	}	
	
	
	@Override
	public boolean equals(final Object obj) {
		if ( ! (obj instanceof FQuota))
			return false;
		
		return compareTo((FQuota) obj) == 0;
	}
	
	@Override
	public int hashCode() {
		return user.hashCode();
	}
}
