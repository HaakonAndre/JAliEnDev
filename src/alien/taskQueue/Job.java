package alien.taskQueue;

import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import lazyj.DBFunctions;


/**
 * @author steffen
 * @since Mar 1, 2011
 */

public class Job  implements Comparable<Job> {
	
		
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
	public String status;
	
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
	 */
	public String command;
	
	/**
	 * merging
	 */
	public String merging;
	
	/**
	 * submitHost
	 */
	public String submitHost;
	
	/**
	 * jdl
	 */
	public String jdl;
	
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
		init(db);
	}
	

	private void init(final DBFunctions db){
		queueId = db.geti("queueId");
		priority = db.geti("priority");
		execHost = db.gets("execHost");
		sent = db.getl("sent");
		split = db.geti("split");
		name = db.gets("name");
		spyurl = db.gets("spyurl");
		commandArg = db.gets("commandArg");
		finished = db.getl("finished");
		masterjob = db.getb("masterjob", false);
		status = db.gets("status");
		splitting = db.geti("splitting");
		node = db.gets("node");
		error = db.geti("error");
		current = db.gets("current");
		received = db.getl("received");
		validate = db.getb("validate",false);
		command = db.gets("command");
		merging = db.gets("merging");
		submitHost = db.gets("submitHost");
		jdl = db.gets("jdl");
		path = db.gets("path");
		site = db.gets("site");
		started = db.getl("started");
		expires = db.geti("expires");
		finalPrice = db.getf("finalPrice");
		effectivePriority = db.getf("effectivePriority");
		price = db.getf("price");
		si2k = db.getf("si2k");
		jobagentId = db.geti("jobagentId");
		agentid = db.geti("agentid");
		notify = db.gets("notify");
		chargeStatus = db.gets("chargeStatus");
		optimized = db.getb("optimized",false);
		mtime = db.getDate("mtime", null);	
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
		if (submitHost==null)
			return null;
		
		int idx = submitHost.indexOf('@');
		
		if (idx<0)
			return null;
		
		return lia.util.StringFactory.get(submitHost.substring(0, idx).toLowerCase());
	}

	private static final Pattern pJDLContent = Pattern.compile("^\\s*\\[\\s*(.*)\\s*\\]\\s*$", Pattern.DOTALL | Pattern.MULTILINE); 
	
	/**
	 * @return the JDL contents, without the enclosing []
	 */
	public String getJDL(){
		String ret = jdl;
		
		final Matcher m = pJDLContent.matcher(ret);
		
		if (m.matches())
			ret = m.group(1);
		
		ret = ret.replaceAll("(^|\\n)\\s{1,8}", "$1");
		
		return ret;
	}
	
}
