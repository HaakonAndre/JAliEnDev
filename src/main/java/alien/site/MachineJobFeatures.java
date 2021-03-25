package alien.site;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.config.ConfigUtils;

/**
 * @author sweisz
 *
 */
public class MachineJobFeatures {

	static transient final Logger logger = ConfigUtils.getLogger(MachineJobFeatures.class.getCanonicalName());
	public static String MachineFeaturesDir = System.getenv().getOrDefault("MACHINEFEATURES", "");
	public static String JobFeaturesDir = System.getenv().getOrDefault("JOBFEATURES", "");

	public static enum FeatureType {
		MACHINEFEATURE, JOBFEATURE;
	}

	/**
	 * @param feature
	 * @return the MJF parameters from files
	 */

	public static String getValueFromFile(String fullPath) {
		String output = null;

		if (fullPath == null) {
			logger.log(Level.WARNING, "Can't resolve path: " + fullPath);
			return null;
		}

		try (FileInputStream fis = new FileInputStream(fullPath)) {
			output = String.valueOf(fis.readAllBytes());
		} catch (IOException e) {
			logger.log(Level.WARNING, "File does not exist: " + fullPath);
			return null;
		}

		return output;
	}

	public static String resolvePath(String featureString, FeatureType type) {
		String resolvedPath = null;

		switch (type) {
		case JOBFEATURE:
			resolvedPath = JobFeaturesDir + "/" + featureString;
			break;
		case MACHINEFEATURE:
			resolvedPath = MachineFeaturesDir + "/" + featureString;
			break;
		default:
			logger.log(Level.WARNING, "No matching feature type");
		}

		return resolvedPath;
	}

	public static String getFeature(String featureString, FeatureType type) {
		String output = null;
		String resolvedPath = null;

		resolvedPath = resolvePath(featureString, type);

		output = getValueFromFile(resolvedPath);

		return output;
	}

	public static String getFeatureOrDefault(String featureString, FeatureType type, String defaultString) {
		String output = getFeature(featureString, type);

		return output != null ? output : defaultString;
	}

	public static Long getFeatureNumber(String featureString, FeatureType type) {
		String output = null;
		String resolvedPath = null;

		resolvedPath = resolvePath(featureString, type);

		output = getValueFromFile(resolvedPath);

		System.out.println("Got value " + output);

		return output != null ? Long.valueOf(output) : null;
	}

	public static Long getFeatureNumberOrDefault(String featureString, FeatureType type, Long defaultNumber) {
		Long output = getFeatureNumber(featureString, type);

		return output != null ? output : defaultNumber;
	}
}
