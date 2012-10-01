package alien.taskQueue;

import java.io.IOException;
import java.io.Serializable;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.security.cert.X509Certificate;

import lazyj.DBFunctions;
import lia.util.StringFactory;


/**
 * @author ron
 * @since Mar 1, 2011
 */

public class Job  implements Comparable<Job>,Serializable {
	
		
	/**
	 * 
	 */
	private static final long serialVersionUID = 7214453241953215533L;

	/**
	 * Job Queue ID
	 */
	public int queueId;
	
	/**
	 * Job Priority 
	 */
	public int priority;
	
	/**
	 * Job exec host 
	 */
	public String execHost;
	
	
	/**
	 * sent
	 */
	public long sent;
	
	/**
	 * split
	 */
	public int split;
	
	/**
	 * name - executable
	 */
	public String name;
	
	/**
	 * URL
	 */
	public String spyurl;
	
	/**
	 * executable parameters
	 */
	public String commandArg;
	
	/**
	 * finished
	 */
	public long finished;
	
	/**
	 * masterjob
	 */
	public boolean masterjob;
	
	/**
	 * Job status
	 */
	private String status;
	
	/**
	 * splitting
	 */
	public int splitting;
	
	/**
	 * node
	 */
	public String node;

	/**
	 * error
	 */
	public int error;
	
	/**
	 * current
	 */
	public String current;

	/**
	 * received
	 */
	public long received;
	
	/**
	 * validate
	 */
	public boolean validate;
	
	/**
	 * command
	 * @deprecated
	 */
	@Deprecated
	public String command;
	
	/**
	 * merging
	 */
	public String merging;
	
	/**
	 * User name
	 */
	public String user;
	
	/**
	 * submitHost
	 */
	public String submitHost;
	
	/**
	 * jdl
	 */
	public String jdl;
	
	/**
	 * the submitter's certificate (public)
	 * 
	 * TODO : X509Certificate objects are not serializable, be careful with this field ...
	 */
	public transient X509Certificate userCertificate;
	
	/**
	 * path
	 */
	public String path;

	/**
	 * site
	 */
	public String site;
	
	/**
	 * started
	 */
	public long started;
	
	/**
	 * expires
	 */
	public int expires;
	
	/**
	 * finalPrice
	 */
	public float finalPrice;
	
	/**
	 * effectivePriority
	 */
	public float effectivePriority;
	
	/**
	 * price
	 */
	public float price;
	
	/**
	 * si2k
	 */
	public float si2k;

	/**
	 * jobagentId
	 */
	public int jobagentId;

	/**
	 * agentid
	 */
	public int agentid;
	
	/**
	 * notify
	 */
	public String notify;
	
	/**
	 * chargeStatus
	 */
	public String chargeStatus;
	
	/**
	 * optimized
	 */
	public boolean optimized;
	
	/**
	 * mtime
	 */
	public Date mtime;
		
	/**
	 * Load one row from a G*L table
	 * 
	 * @param db
	 */
	Job(final DBFunctions db){
		init(db, false);
	}
	
	/**
	 * Fake a job, needs to be removed one day!
	 */
	public Job(){
		// nothing
	}
	
	/**
	 * @param db
	 * @param loadJDL
	 */
	Job(final DBFunctions db, final boolean loadJDL){
		init(db, loadJDL);
	}
	
	private void init(final DBFunctions db, final boolean loadJDL){
		queueId = db.geti("queueId");
		priority = db.geti("priority");
		sent = db.getl("sent");
		split = db.geti("split");
		spyurl = StringFactory.get(db.gets("spyurl"));
		commandArg = StringFactory.get(db.gets("commandArg", null));
		finished = db.getl("finished");
		masterjob = db.getb("masterjob", false);
		splitting = db.geti("splitting");
		error = db.geti("error");
		current = StringFactory.get(db.gets("current", null));
		received = db.getl("received");
		validate = db.getb("validate",false);
		merging = StringFactory.get(db.gets("merging", null));
		jdl = loadJDL ? db.gets("jdl") : null;
		path = StringFactory.get(db.gets("path", null));
		site = StringFactory.get(db.gets("site", null));
		started = db.getl("started");
		expires = db.geti("expires");
		finalPrice = db.getf("finalPrice");
		effectivePriority = db.getf("effectivePriority");
		price = db.getf("price");
		si2k = db.getf("si2k");
		jobagentId = db.geti("jobagentId");
		agentid = db.geti("agentid");		
		chargeStatus = StringFactory.get(db.gets("chargeStatus", null));
		optimized = db.getb("optimized",false);
		mtime = db.getDate("mtime", null);	
		
		if (TaskQueueUtils.dbStructure2_20){
			status = TaskQueueUtils.codeToStatus.get(Integer.valueOf(db.geti("statusId")));
			submitHost = TaskQueueUtils.getHost(db.geti("submitHostId"));
			execHost = TaskQueueUtils.getHost(db.geti("execHostId"));
			node = TaskQueueUtils.getHost(db.geti("nodeid"));
			notify = TaskQueueUtils.getNotify(db.geti("notifyId"));
			name = command = TaskQueueUtils.getCommand(db.geti("commandId"));
		}
		else{
			status = StringFactory.get(db.gets("status"));
			submitHost = db.gets("submitHost");
			execHost = StringFactory.get(db.gets("execHost"));
			node = StringFactory.get(db.gets("node", null));
			notify = StringFactory.get(db.gets("notify", null));
			name = command = StringFactory.get(db.gets("command", null));
			
			final int idx = submitHost.indexOf('@');
			
			if (idx>0){
				user = StringFactory.get(submitHost.substring(0, idx));
				submitHost = StringFactory.get(submitHost.substring(idx+1));
			}
			else{
				submitHost = StringFactory.get(submitHost);
			}
		}
	}

	@Override
	public int compareTo(final Job o) {
		return queueId - o.queueId;
	}
	
	@Override
	public boolean equals(final Object obj) {
		if (! (obj instanceof Job))
			return false;
		
		return compareTo((Job) obj) == 0;
	}
	
	@Override
	public int hashCode() {
		return queueId;
	}

	
	@Override		
	public String toString() {
		return "Job queueId\t\t: "+queueId+"\n" +
		" priority\t\t: "+priority+"\n" +
		" execHost\t\t: "+execHost+"\n" +
		" sent\t\t: "+sent+"\n"+
		" split\t\t: "+split+"\n" +
		" name\t\t: "+name+"\n" +
		" spyurl\t\t: "+spyurl+"\n" +
		" commandArg\t\t: "+commandArg+"\n" +
		" finished\t\t: "+finished+"\n" +
		" masterjob\t\t: "+masterjob+"\n" +
		" status\t\t: "+status+"\n" +
		" splitting\t\t: "+splitting+"\n" +
		" node\t\t: "+node+"\n" +
		" error\t\t: "+error+"\n" +
		" current\t\t: "+current+"\n" +
		" received\t\t: "+received+"\n" +
		" validate\t\t: "+validate+"\n" +
		" command\t\t: "+command+"\n" +
		" merging\t\t: "+merging+"\n" +
		" user\t\t: "+user+"\n"+
		" submitHost\t\t: "+submitHost+"\n" +
		" jdl\t\t: "+jdl+"\n" +
		" path\t\t: "+path+"\n" +
		" site\t\t: "+site+"\n" +
		" started\t\t: "+started+"\n" +
		" expires\t\t: "+expires+"\n" +
		" finalPrice\t\t: "+finalPrice+"\n" +
		" effectivePriority\t\t: "+effectivePriority+"\n" +
		" price\t\t: "+price+"\n" +
		" si2k\t\t: "+si2k+"\n" +
		" jobagentId\t\t: "+jobagentId+"\n" +
		" agentid\t\t: "+agentid+"\n" +
		" notify\t\t: "+notify+"\n" +
		" chargeStatus\t\t: "+chargeStatus+"\n" +
		" optimized\t\t: "+optimized+"\n" +
		" mtime\t\t: "+mtime+ "\n";
	}
	
	/**
	 * @return the owner of the job (AliEn account name)
	 */
	public String getOwner(){
		return user;
	}

	private static final Pattern pJDLContent = Pattern.compile("^\\s*\\[\\s*(.*)\\s*\\]\\s*$", Pattern.DOTALL | Pattern.MULTILINE); 
	
	/**
	 * @return original JDL as in the QUEUE table
	 */
	public String getOriginalJDL(){
		if (jdl==null){
			jdl = TaskQueueUtils.getJDL(queueId);
			
			if (jdl==null)
				return "";
		}
		
		return jdl;
	}
	
	/**
	 * @param jdlContent
	 * @return the JDL content without the enclosing [] if any
	 */
	public static String sanitizeJDL(final String jdlContent){
		String ret = jdlContent;
		
		final Matcher m = pJDLContent.matcher(ret);
		
		if (m.matches())
			ret = m.group(1);
		
		ret = ret.replaceAll("(^|\\n)\\s{1,8}", "$1");
		
		return ret;
	}
	
	/**
	 * @return the JDL contents, without the enclosing []
	 */
	public String getJDL(){
		return sanitizeJDL(getOriginalJDL());
	}
	
	
	/**
	 * @return the status, as object
	 */
	public JobStatus status(){
		return JobStatus.getStatus(status);
	}
	
	/**
	 * @return status name
	 */
	public String getStatusName(){
		return status;
	}
	
	/**
	 * @return <code>true</code> if the job has finished successfully
	 */
	public boolean isDone(){
		return status.startsWith("DONE");
	}
	
	/**
	 * @return <code>true</code> if the job is in a final error state
	 */
	public boolean isError(){
		return status.startsWith("ERR") || status.startsWith("EXP") || status.startsWith("KILL");
	}
	
	/**
	 * @return <code>true</code> if the job is in a final state (either successful or failed)
	 */
	public boolean isFinalState(){
		return isDone() || isError();
	}
	
	/**
	 * @return <code>true</code> if the job is still active
	 */
	public boolean isActive(){
		return !isFinalState();
	}
	
	
	/**
	 * @return <code>true</code> if the job is a master job
	 */
	public boolean isMaster(){
		return masterjob;
	}
	
	
	/**
	 * @return <code>true</code> if the job has a validation flag
	 */
	public boolean usesValidation(){
		//
		//		TODO:
		//		grep the JDL for the validation flag, perl did the following:
		//	    $data->{jdl} =~ /validate\s*=\s*1/i and $validate = 1;

		try{
			final JDL j = new JDL(jdl);
			
			return Integer.parseInt(j.gets("validate")) == 1;
		}
		catch (IOException ioe){
			// ignore
		}
		catch (NumberFormatException nfe){
			// ignore
		}
		catch (NullPointerException npe){
			// ignore
		}
		
		return false;
	}
}
