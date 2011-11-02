package alien.taskQueue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

@SuppressWarnings("javadoc")


/**
 * @author ron
 * @since Mar 1, 2011
 */
public enum JobStatus {

	INSERTING("INSERTING"), SPLITTING("SPLITTING"), SPLIT("SPLIT"), TO_STAGE(
			"TO_STAGE"), A_STAGED("A_STAGED"), STAGING("STAGING"), WAITING(
			"WAITING"), OVER_WAITING("OVER_WAITING"), ASSIGNED("ASSIGNED"), QUEUED(
			"QUEUED"), STARTED("STARTED"), IDLE("IDLE"), INTERACTIV(
			"INTERACTIV"), RUNNING("RUNNING"), SAVING("SAVING"), SAVED("SAVED"), DONE(
			"DONE"), SAVED_WARN("SAVED_WARN"), DONE_WARN("DONE_WARN"), ERROR_A(
			"ERROR_A"), ERROR_I("ERROR_I"), ERROR_E("ERROR_E"), ERROR_IB(
			"ERROR_IB"), ERROR_M("ERROR_M"), ERROR_RE("ERROR_RE"), ERROR_S(
			"ERROR_S"), ERROR_SV("ERROR_SV"), ERROR_V("ERROR_V"), ERROR_VN(
			"ERROR_VN"), ERROR_VT("ERROR_VT"), ERROR_SPLT("ERROR_SPLT"), EXPIRED(
			"EXPIRED"), FAILED("FAILED"), KILLED("KILLED"), FORCEMERGE(
			"FORCEMERGE"), MERGING("MERGING"), ZOMBIE("ZOMBIE"),
			//
			ANY("%");

	private final static HashMap<String,Integer> jobstatuslevel  = new HashMap<String,Integer>();

	
	static{
		jobstatuslevel.put("INSERTING",10);
		jobstatuslevel.put("SPLITTING",15);
		jobstatuslevel.put("SPLIT",18);
		jobstatuslevel.put("TO_STAGE",16);
		jobstatuslevel.put("A_STAGED",17);
		jobstatuslevel.put("STAGING",19);
		jobstatuslevel.put("WAITING",20);
		jobstatuslevel.put("OVER_WAITING",21);
		jobstatuslevel.put("ASSIGNED",25);
		jobstatuslevel.put("QUEUED",30);
		jobstatuslevel.put("STARTED",40);
		jobstatuslevel.put("IDLE",50);
		jobstatuslevel.put("INTERACTIV",50);
		jobstatuslevel.put("RUNNING",50);
		jobstatuslevel.put("SAVING",60);
		jobstatuslevel.put("SAVED",70);
		jobstatuslevel.put("DONE",980);
		jobstatuslevel.put("SAVED_WARN",71);
		jobstatuslevel.put("DONE_WARN",981);
		jobstatuslevel.put("ERROR_A",990);
		jobstatuslevel.put("ERROR_I",990);
		jobstatuslevel.put("ERROR_E",990);
		jobstatuslevel.put("ERROR_IB",990);
		jobstatuslevel.put("ERROR_M",990);
		jobstatuslevel.put("ERROR_RE",990);
		jobstatuslevel.put("ERROR_S",990);
		jobstatuslevel.put("ERROR_SV",990);
		jobstatuslevel.put("ERROR_V",990);
		jobstatuslevel.put("ERROR_VN",990);
		jobstatuslevel.put("ERROR_VT",990);
		jobstatuslevel.put("ERROR_SPLT",990);
		jobstatuslevel.put("EXPIRED",1000);
		jobstatuslevel.put("FAILED",1000);
		jobstatuslevel.put("KILLED",1001);
		jobstatuslevel.put("FORCEMERGE",950);
		jobstatuslevel.put("MERGING",970);
		jobstatuslevel.put("ZOMBIE",999);
		//
		jobstatuslevel.put("ANY",-1);
	};

	
	
	public static JobStatus get(final String status){
		if(status.equals("INSERTING")) return JobStatus.INSERTING;
		else if(status.equals("SPLITTING")) return JobStatus.SPLITTING;
		else if(status.equals("SPLIT")) return JobStatus.SPLIT;
		else if(status.equals("TO_STAGE")) return JobStatus.TO_STAGE;
		else if(status.equals("A_STAGED")) return JobStatus.A_STAGED;
		else if(status.equals("STAGING")) return JobStatus.STAGING;
		else if(status.equals("WAITING")) return JobStatus.WAITING;
		else if(status.equals("OVER_WAITING")) return JobStatus.OVER_WAITING;
		else if(status.equals("ASSIGNED")) return JobStatus.ASSIGNED;
		else if(status.equals("QUEUED")) return JobStatus.QUEUED;
		else if(status.equals("STARTED")) return JobStatus.STARTED;
		else if(status.equals("IDLE")) return JobStatus.IDLE;
		else if(status.equals("INTERACTIV")) return JobStatus.INTERACTIV;
		else if(status.equals("RUNNING")) return JobStatus.RUNNING;
		else if(status.equals("SAVING")) return JobStatus.SAVING;
		else if(status.equals("SAVED")) return JobStatus.SAVED;
		else if(status.equals("DONE")) return JobStatus.DONE;
		else if(status.equals("SAVED_WARN")) return JobStatus.SAVED_WARN;
		else if(status.equals("DONE_WARN")) return JobStatus.DONE_WARN;
		else if(status.equals("ERROR_A")) return JobStatus.ERROR_A;
		else if(status.equals("ERROR_I")) return JobStatus.ERROR_I;
		else if(status.equals("ERROR_E")) return JobStatus.ERROR_E;
		else if(status.equals("ERROR_IB")) return JobStatus.ERROR_IB;
		else if(status.equals("ERROR_M")) return JobStatus.ERROR_M;
		else if(status.equals("ERROR_RE")) return JobStatus.ERROR_RE;
		else if(status.equals("ERROR_S")) return JobStatus.ERROR_S;
		else if(status.equals("ERROR_SV")) return JobStatus.ERROR_SV;
		else if(status.equals("ERROR_V")) return JobStatus.ERROR_V;
		else if(status.equals("ERROR_VN")) return JobStatus.ERROR_VN;
		else if(status.equals("ERROR_VT")) return JobStatus.ERROR_VT;
		else if(status.equals("ERROR_SPLT")) return JobStatus.ERROR_SPLT;
		else if(status.equals("EXPIRED")) return JobStatus.EXPIRED;
		else if(status.equals("FAILED")) return JobStatus.FAILED;
		else if(status.equals("KILLED")) return JobStatus.KILLED;
		else if(status.equals("FORCEMERGE")) return JobStatus.FORCEMERGE;
		else if(status.equals("MERGING")) return JobStatus.MERGING;
		else if(status.equals("ZOMBIE")) return JobStatus.ZOMBIE;
		else return null;
	}
	
	
	

	private final String status;

	JobStatus(final String status) {
		this.status = status;
	}
	
	/**
	 * Is this job status older/more final than the other one
	 */
	public boolean biggerThan(final JobStatus another){
		return (jobstatuslevel.get(this)>jobstatuslevel.get(another));
	}
	
	/**
	 * Is this job status older/more final than or equals the other one
	 */
	public boolean biggerThanEquals(final JobStatus another){
		return (jobstatuslevel.get(this)>=jobstatuslevel.get(another));
	}

	/**
	 * Is this job status younger/less final than the other one
	 */
	public boolean smallerThan(final JobStatus another){
		return (jobstatuslevel.get(this)<jobstatuslevel.get(another));
	}

	/**
	 * Is this job status younger/less final than or equals the other one
	 */
	public boolean smallerThanEquals(final JobStatus another){
		return (jobstatuslevel.get(this)<=jobstatuslevel.get(another));
	}
	

	/**
	 * Does this job status equals the other one
	 */
	public boolean equals(final JobStatus another){
		return (jobstatuslevel.get(this)==jobstatuslevel.get(another));
	}
	
	/**
	 * Is this status equal to another one
	 */
	public boolean is(final JobStatus another){
		return equals(another);
	}
	
	/**
	 * Id this status a ERROR_*
	 */
	public boolean isERROR_(){
		return status.startsWith("ERROR_");
	}
	
	/**
	 * Is this status a n error state: ERROR_*|FAILED
	 */
	public boolean isErrorState(){
		if(isERROR_() || is(FAILED) )
			return true;
		return false;
	}
	
	private static final List<JobStatus> errorneousStates = Arrays.asList(
			JobStatus.ERROR_A, JobStatus.ERROR_I, JobStatus.ERROR_E,
			JobStatus.ERROR_IB, JobStatus.ERROR_M, JobStatus.ERROR_RE,
			JobStatus.ERROR_S, JobStatus.ERROR_SV, JobStatus.ERROR_V,
			JobStatus.ERROR_VN, JobStatus.ERROR_VT, JobStatus.ERROR_SPLT,
			JobStatus.EXPIRED, JobStatus.FAILED);
	
	/**
	 * All error_*, expired and failed states
	 */
	public static List<JobStatus> errorneousStates(){
		return errorneousStates();
	}
	

	private static final List<JobStatus> runningStates = Arrays.asList(
			JobStatus.RUNNING, JobStatus.STARTED, JobStatus.SAVING);
	
	/**
	 * All running states 
	 */
	public static List<JobStatus> runningStates(){
		return runningStates();
	}
	

	private static final List<JobStatus> queuedStates = Arrays.asList(
			JobStatus.QUEUED, JobStatus.ASSIGNED);
	
	/**
	 * All queued states 
	 */
	public static List<JobStatus> queuedStates(){
		return queuedStates();
	}

	/**
	 * All queued states 
	 */
	public static List<JobStatus> finalStates(){
		List<JobStatus> err = errorneousStates;
		err.add(DONE);
		return err;
	}
	
	private static final List<JobStatus> waitingStates = Arrays.asList(
			JobStatus.INSERTING, JobStatus.EXPIRED, JobStatus.WAITING, JobStatus.ASSIGNED, JobStatus.QUEUED);
	/**
	 * All waiting states
	 */
	public static List<JobStatus> waitingStates(){
		return waitingStates;
	}
	
	/**
	 * The level/index/age of this job status
	 */
	public int level(){
		return jobstatuslevel.get(status);
	}

	@Override
	public String toString() {
		return status;
	}
}
