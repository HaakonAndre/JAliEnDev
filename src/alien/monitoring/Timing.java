/**
 * 
 */
package alien.monitoring;

import java.util.Vector;

/**
 * @author costing
 * @since Nov 10, 2010
 */
public class Timing implements MonitoringObject {
	
	private String name;
	
	private int count = 0;
	private double sum = 0;
	private double min = 0;
	private double max = 0;
	
	private long lastRate = System.currentTimeMillis();
	
	/**
	 * @param name 
	 */
	public Timing(final String name) {
		this.name = name;
	}
	
	/**
	 * Add a timing measurement
	 * 
	 * @param millis
	 */
	public synchronized void addTiming(final double millis){
		count++;
		sum += millis;
		
		if (count==1){
			min = max = millis;
		}
		else{
			min = Math.min(min, millis);
			max = Math.max(max, millis);
		}
	}
	
	/* (non-Javadoc)
	 * @see alien.monitoring.MonitoringObject#fillValues(java.util.Vector, java.util.Vector)
	 */
	@Override
	public synchronized void fillValues(final Vector<String> paramNames, final Vector<Object> paramValues) {
		paramNames.add(name+"_sum");
		paramValues.add(Double.valueOf(sum));

		paramNames.add(name+"_cnt");
		paramValues.add(Integer.valueOf(count));
		
		final long now = System.currentTimeMillis();
		
		final long diff = now - lastRate;
		
		lastRate = now;
		
		if (count>0){
			paramNames.add(name+"_avg");
			paramValues.add(Double.valueOf(sum / count));
			
			paramNames.add(name+"_min");
			paramValues.add(Double.valueOf(min));

			paramNames.add(name+"_max");
			paramValues.add(Double.valueOf(max));			
		}
		
		if (diff > 0){
			paramNames.add(name+"_R");
			paramValues.add(Double.valueOf(sum*1000d / diff));

			paramNames.add(name+"_cnt_R");
			paramValues.add(Double.valueOf(count*1000d / diff));
		}
		
		count = 0;
		sum = 0;
	}

}
