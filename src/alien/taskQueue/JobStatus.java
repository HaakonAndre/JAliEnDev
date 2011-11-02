package alien.taskQueue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @author ron
 * @since Mar 1, 2011
 */
public enum JobStatus {

	/**
	 * Inserting job
	 */
	INSERTING(10),
	/**
	 * Splitting 
	 */
	SPLITTING(15),
	/**
	 * Split job
	 */
	SPLIT(18),
	/**
	 * Waiting for files to be staged
	 */
	TO_STAGE(16),
	/**
	 * ?
	 */
	A_STAGED(17),
	/**
	 * Currently staging
	 */
	STAGING(19),
	/**
	 * Waiting to be picked up
	 */
	WAITING(20),
	/**
	 * User ran out of quota
	 */
	OVER_WAITING(21), 
	/**
	 * Assigned to a site
	 */
	ASSIGNED(25),
	/**
	 * Queued
	 */
	QUEUED(30),
	/**
	 * Job agent has started the job
	 */
	STARTED(40),
	/**
	 * Idle, doing what ? 
	 */
	IDLE(50),
	/**
	 * ha ?
	 */
	INTERACTIV(50),
	/**
	 * Currently running on the WN
	 */
	RUNNING(50),
	/**
	 * Saving its outpt
	 */
	SAVING(60),
	/**
	 * Output saved, waiting for CS to acknowledge this
	 */
	SAVED(70),
	/**
	 * Fantastic, job successfully completed
	 */
	DONE(980),
	/**
	 * Files were saved, with some errors
	 */
	SAVED_WARN(71),
	/**
	 * Job is successfully done, but saving saw some errors
	 */
	DONE_WARN(981),
	/**
	 * ERROR_A
	 */
	ERROR_A(990),
	/**
	 * Error inserting
	 */
	ERROR_I(990),
	/**
	 * Error executing (over TTL, memory limits etc)
	 */
	ERROR_E(990),
	/**
	 * Error downloading the input files
	 */
	ERROR_IB(990),
	/**
	 * Error merging
	 */
	ERROR_M(990),
	/**
	 * Error registering
	 */
	ERROR_RE(990),
	/**
	 * ERROR_S
	 */
	ERROR_S(990),
	/**
	 * Error saving output files
	 */
	ERROR_SV(990),
	/**
	 * Validation error
	 */
	ERROR_V(990),
	/**
	 * Cannot run the indicated validation code
	 */
	ERROR_VN(990),
	/**
	 * ERROR_VT
	 */
	ERROR_VT(990),
	/**
	 * Waiting time expired (LPMActivity JDL tag)
	 */
	ERROR_W(990),
	/**
	 * Error splitting
	 */
	ERROR_SPLT(990),
	/**
	 * Job didn't report for too long
	 */
	EXPIRED(1000),
	/**
	 * Failed
	 */
	FAILED(1000),
	/**
	 * Terminated
	 */
	KILLED(1001),
	/**
	 * Force merge
	 */
	FORCEMERGE(950),
	/**
	 * Currently merging 
	 */
	MERGING(970),
	/**
	 * Zombie
	 */
	ZOMBIE(999),
	
	/**
	 * Any state (wildcard)
	 */
	ANY(-1);

	private final int level;

	private JobStatus(final int level) {
		this.level = level;
		
		JobStatusFactory.addStatus(this);
	}
	
	/**
	 * Is this job status older/more final than the other one
	 * @param another 
	 * @return true if state is larger than
	 */
	public boolean biggerThan(final JobStatus another){
		return level > another.level;
	}
	
	/**
	 * Is this job status older/more final than or equals the other one
	 * @param another 
	 * @return true if the state is larger or equal with
	 */
	public boolean biggerThanEquals(final JobStatus another){
		return this.level >= another.level;
	}

	/**
	 * Is this job status younger/less final than the other one
	 * @param another 
	 * @return level is smaller than
	 */
	public boolean smallerThan(final JobStatus another){
		return this.level < another.level;
	}

	/**
	 * Is this job status younger/less final than or equals the other one
	 * @param another 
	 * @return level is smaller or equal with
	 */
	public boolean smallerThanEquals(final JobStatus another){
		return this.level <= another.level;
	}

	/**
	 * Does this job status equals the other one
	 * @param another 
	 * @return true if level is identical in both
	 */
	public boolean equals(final JobStatus another){
		return this.level == another.level;
	}
	
	/**
	 * Is this status equal to another one
	 * @param another 
	 * @return true if the level is identical
	 */
	public boolean is(final JobStatus another){
		return equals(another);
	}
	
	/**
	 * Id this status a ERROR_
	 * @return true if this is any ERROR_ state
	 */
	public boolean isERROR_(){
		return name().startsWith("ERROR_");
	}
	
	/**
	 * Is this status a n error state: ERROR_*|FAILED
	 * @return true if any error state
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
			JobStatus.ERROR_W, JobStatus.EXPIRED, JobStatus.FAILED);
	
	/**
	 * All error_*, expired and failed states
	 * @return true if any
	 */
	public static List<JobStatus> errorneousStates(){
		return errorneousStates();
	}
	

	private static final List<JobStatus> runningStates = Arrays.asList(
			JobStatus.RUNNING, JobStatus.STARTED, JobStatus.SAVING);
	
	/**
	 * All running states 
	 * @return the list of active states
	 */
	public static List<JobStatus> runningStates(){
		return runningStates;
	}
	

	private static final List<JobStatus> queuedStates = Arrays.asList(
			JobStatus.QUEUED, JobStatus.ASSIGNED);
	
	/**
	 * All queued states 
	 * @return the list of queued states
	 */
	public static List<JobStatus> queuedStates(){
		return queuedStates;
	}

	private static final List<JobStatus> finalStates;
	
	static{
		final List<JobStatus> temp = new ArrayList<JobStatus>(errorneousStates);
		temp.add(DONE);
		temp.add(DONE_WARN);
		temp.add(KILLED);
		
		finalStates = Collections.unmodifiableList(temp);
	}
	
	/**
	 * All queued states 
	 * @return the list of error states
	 */
	public static List<JobStatus> finalStates(){
		return finalStates;
	}
	
	private static final List<JobStatus> waitingStates = Arrays.asList(
			JobStatus.INSERTING, JobStatus.EXPIRED, JobStatus.WAITING, JobStatus.ASSIGNED, JobStatus.QUEUED);
	/**
	 * All waiting states
	 * @return waiting states
	 */
	public static List<JobStatus> waitingStates(){
		return waitingStates;
	}
	
	/**
	 * The level/index/age of this job status
	 * @return numeric level
	 */
	public int level(){
		return level;
	}

	@Override
	public String toString() {
		return name();
	}
}
