package alien.site.containers;

import java.util.logging.Level;
import java.util.logging.Logger;
import alien.config.ConfigUtils;
import alien.site.JobAgent;

public class ContainerizerFactory {
	
	static transient final Logger logger = ConfigUtils.getLogger(JobAgent.class.getCanonicalName());

	enum Containerizers {
		Singularity,
		Docker
	}

	public static Containerizer getContainerizer(){
		for (Containerizers c : Containerizers.values()) { 
			try {
				Containerizer containerizerCandidate = (Containerizer) getClassFromName(c.name()).getConstructor().newInstance();
				if(containerizerCandidate.isSupported())
					return containerizerCandidate;			
			}catch(Exception e) {
				logger.log(Level.WARNING, "Invalid containerizer: " + e);
			}
		}
		return null;
	}
	
	private static Class<?> getClassFromName(String name) throws ClassNotFoundException {	
		String pkg = Containerizer.class.getPackageName();
		return Class.forName(pkg + "." + name);
	}
}