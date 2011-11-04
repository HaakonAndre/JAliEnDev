package alien.taskQueue;

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author ron
 * @since Mar 1, 2011
 */
public enum JobStatus{

	/**
	 * Any state (wildcard)
	 */
	ANY(-1),
	/**
	 * Inserting job (10)
	 */
	INSERTING(10),
	/**
	 * Splitting (15)
	 */
	SPLITTING(15),
	/**
	 * Waiting for files to be staged (16)
	 */
	TO_STAGE(16),
	/**
	 * (17)
	 */
	A_STAGED(17),
	/**
	 * Split job (18)
	 */
	SPLIT(18),
	/**
	 * Currently staging (19)
	 */
	STAGING(19),
	/**
	 * Waiting to be picked up (20)
	 */
	WAITING(20),
	/**
	 * User ran out of quota (21)
	 */
	OVER_WAITING(21), 
	/**
	 * Assigned to a site (25)
	 */
	ASSIGNED(25),
	/**
	 * Queued (30)
	 */
	QUEUED(30),
	/**
	 * Job agent has started the job (40)
	 */
	STARTED(40),
	/**
	 * Idle, doing what ? (50) 
	 */
	IDLE(45),
	/**
	 * (50)
	 */
	INTERACTIV(46),
	/**
	 * Currently running on the WN (50)
	 */
	RUNNING(50),
	/**
	 * Saving its output (60)
	 */
	SAVING(60),
	/**
	 * Output saved, waiting for CS to acknowledge this (70)
	 */
	SAVED(70),
	/**
	 * Files were saved, with some errors (71)
	 */
	SAVED_WARN(71),
	/**
	 * Zombie (999)
	 */
	ZOMBIE(600),
	/**
	 * Force merge (950)
	 */
	FORCEMERGE(700),
	/**
	 * Currently merging  (970)
	 */
	MERGING(701),
	/**
	 * Fantastic, job successfully completed (980)
	 */
	DONE(800),
	/**
	 * Job is successfully done, but saving saw some errors (981)
	 */
	DONE_WARN(801),
	/**
	 * ERROR_A (990)
	 */
	ERROR_A(900),
	/**
	 * Error inserting (990)
	 */
	ERROR_I(901),
	/**
	 * Error executing (over TTL, memory limits etc) (990)
	 */
	ERROR_E(902),
	/**
	 * Error downloading the input files (990)
	 */
	ERROR_IB(903),
	/**
	 * Error merging (990)
	 */
	ERROR_M(904),
	/**
	 * Error registering (990)
	 */
	ERROR_RE(905),
	/**
	 * ERROR_S (990)
	 */
	ERROR_S(906),
	/**
	 * Error saving output files (990)
	 */
	ERROR_SV(907),
	/**
	 * Validation error (990)
	 */
	ERROR_V(908),
	/**
	 * Cannot run the indicated validation code (990)
	 */
	ERROR_VN(909),
	/**
	 * ERROR_VT (990)
	 */
	ERROR_VT(910),
	/**
	 * Waiting time expired (LPMActivity JDL tag) (990)
	 */
	ERROR_W(911),
	/**
	 * Error splitting (990)
	 */
	ERROR_SPLT(912),
	/**
	 * Job didn't report for too long (1000)
	 */
	EXPIRED(1000),
	/**
	 * Failed (1000)
	 */
	FAILED(1001),
	/**
	 * Terminated (1001)
	 */
	KILLED(1002);	

	private final int level;

	private JobStatus(final int level) {
		this.level = level;
	}
	
	private static Map<String, JobStatus> stringToStatus = new HashMap<String, JobStatus>();
	
	private static Map<Integer, JobStatus> intToStatus = new HashMap<Integer, JobStatus>();
	
	static {
		for (final JobStatus status: JobStatus.values()){
			stringToStatus.put(status.name(), status);
			
			intToStatus.put(Integer.valueOf(status.level()), status);
		}
		
		stringToStatus.put("%", ANY);
	}
	
	/**
	 * @param status
	 * @return the status indicated by this name
	 */
	public static final JobStatus getStatus(final String status){
		return stringToStatus.get(status);
		
	}
	
	/**
	 * @param level
	 * @return the status indicated by this level
	 */
	public static final JobStatus getStatus(final Integer level){
		return intToStatus.get(level);
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
	 * Id this status a ERROR_
	 * @return true if this is any ERROR_ state
	 */
	public boolean isERROR_(){
		return level>=900 && level<1000;
	}
	
	/**
	 * Is this status a n error state: ERROR_*|FAILED
	 * @return true if any error state
	 */
	public boolean isErrorState(){
		return isERROR_() || this==FAILED;
	}
	
	private static final Set<JobStatus> errorneousStates = Collections.unmodifiableSet(EnumSet.range(ERROR_A, FAILED)); 
			
	/**
	 * All error_*, expired and failed states
	 * @return true if any
	 */
	public static final Set<JobStatus> errorneousStates(){
		return errorneousStates;
	}
	

	private static final Set<JobStatus> runningStates = Collections.unmodifiableSet(EnumSet.of(RUNNING, STARTED, SAVING));
	
	/**
	 * All running states 
	 * @return the set of active states
	 */
	public static final Set<JobStatus> runningStates(){
		return runningStates;
	}
	

	private static final Set<JobStatus> queuedStates = Collections.unmodifiableSet(EnumSet.of(QUEUED, ASSIGNED));
	
	/**
	 * All queued states 
	 * @return the set of queued states
	 */
	public static final Set<JobStatus> queuedStates(){
		return queuedStates;
	}

	private static final Set<JobStatus> finalStates = Collections.unmodifiableSet(EnumSet.range(DONE, KILLED));
	
	/**
	 * All queued states 
	 * @return the set of error states
	 */
	public static final Set<JobStatus> finalStates(){
		return finalStates;
	}
	
	private static final Set<JobStatus> doneStates = Collections.unmodifiableSet(EnumSet.range(DONE, DONE_WARN));
	
	/**
	 * @return done states
	 */
	public static final Set<JobStatus> doneStates(){
		return doneStates;
	}
	
	private static final Set<JobStatus> waitingStates = Collections.unmodifiableSet(EnumSet.of(INSERTING, EXPIRED, WAITING, ASSIGNED, QUEUED));
	
	/**
	 * All waiting states
	 * @return waiting states
	 */
	public static final Set<JobStatus> waitingStates(){
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
	
	/**
	 * @return the SQL selection for this level only
	 */
	public String toSQL(){
		if (level==-1)
			return "%";
		
		return name();
	}
}
