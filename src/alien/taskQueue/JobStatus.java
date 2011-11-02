package alien.taskQueue;

import java.util.Arrays;
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
	SPLIT(18),
	TO_STAGE(16),
	A_STAGED(17),
	STAGING(19),
	WAITING(20),
	OVER_WAITING(21), 
	ASSIGNED(25),
	QUEUED(30),
	STARTED(40),
	IDLE(50),
	INTERACTIV(50),
	RUNNING(50),
	SAVING(60),
	SAVED(70),
	DONE(980),
	SAVED_WARN(71),
	DONE_WARN(981),
	ERROR_A(990),
	ERROR_I(990),
	ERROR_E(990),
	ERROR_IB(990),
	ERROR_M(990),
	ERROR_RE(990),
	ERROR_S(990),
	ERROR_SV(990),
	ERROR_V(990),
	ERROR_VN(990),
	ERROR_VT(990),
	ERROR_W(990),
	ERROR_SPLT(990),
	EXPIRED(1000),
	FAILED(1000),
	KILLED(1001),
	FORCEMERGE(950),
	MERGING(970),
	ZOMBIE(999),
	
	ANY(-1);

	private final int level;

	private JobStatus(final int level) {
		this.level = level;
		
		JobStatusFactory.addStatus(this);
	}
	
	/**
	 * Is this job status older/more final than the other one
	 */
	public boolean biggerThan(final JobStatus another){
		return level > another.level;
	}
	
	/**
	 * Is this job status older/more final than or equals the other one
	 */
	public boolean biggerThanEquals(final JobStatus another){
		return this.level >= another.level;
	}

	/**
	 * Is this job status younger/less final than the other one
	 */
	public boolean smallerThan(final JobStatus another){
		return this.level < another.level;
	}

	/**
	 * Is this job status younger/less final than or equals the other one
	 */
	public boolean smallerThanEquals(final JobStatus another){
		return this.level <= another.level;
	}

	/**
	 * Does this job status equals the other one
	 */
	public boolean equals(final JobStatus another){
		return this.level == another.level;
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
		return name().startsWith("ERROR_");
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
		return level;
	}

	@Override
	public String toString() {
		return name();
	}
}
