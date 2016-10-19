package alien.site.supercomputing.titan;

public class ProcInfoPair{
	public final long queue_id;
	public final String procinfo;
	public ProcInfoPair(String queue_id, String procinfo){
		this.queue_id = Long.parseLong(queue_id);
		this.procinfo = procinfo;
	}
}
