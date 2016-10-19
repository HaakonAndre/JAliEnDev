package alien.site.supercomputing.titan;

public class TitanJobStatus{
	public final int rank;
	public Long queueId;
	public String  jobFolder;
	public String status;
	public int executionCode;
	public int validationCode;
	final TitanBatchInfo batch;
	public TitanJobStatus(int r, Long qid, String job_folder, 
			String st, int exec_code, 
			int val_code, TitanBatchInfo bi){
		rank = r;
		queueId = qid;
		jobFolder = job_folder;
		status = st;
		executionCode = exec_code;
		validationCode = val_code;
		batch = bi;
	}
};
