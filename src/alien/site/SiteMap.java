package alien.site;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import alien.config.ConfigUtils;
import alien.site.packman.CVMFS;
import alien.site.packman.PackMan;

public class SiteMap {

	static transient final Logger logger = ConfigUtils.getLogger(SiteMap.class.getCanonicalName());

	public HashMap<String, Object> siteMap = new HashMap<>();

	/**
	 * @return the site parameters to send to the job broker (packages, ttl, ce/site...)
	 */
	public HashMap<String, Object> getSiteParameters(final Map<String, String> env) {
		if (env == null)
			return null;

		logger.log(Level.INFO, "Getting site map");

		// Local vars
		PackMan packMan = null;
		int origTtl;
		String partition = "";
		String ceRequirements = "";
		List<String> packages;
		List<String> installedPackages;
		ArrayList<String> extrasites = new ArrayList<>();
		String site = null;
		String ce = null;

		// Get hostname
		String hostName = "";
		try {
			hostName = InetAddress.getLocalHost().getCanonicalHostName();
			hostName = hostName.replace("/.$/", "");
		} catch (final UnknownHostException e) {
			logger.severe("Couldn't get hostname");
			e.printStackTrace();
		}
		siteMap.put("Host", hostName);

		// ALIEN_CM_AS_LDAP_PROXY to send messages upstream through VoBox (no really used anymore in JAliEn?)
		String alienCm = hostName;
		if (env.containsKey("ALIEN_CM_AS_LDAP_PROXY"))
			alienCm = env.get("ALIEN_CM_AS_LDAP_PROXY");

		siteMap.put("alienCm", alienCm);

		// Getting PackMan instance and packages
		String installationMethod = "CVMFS";
		if (env.containsKey("installationMethod"))
			installationMethod = env.get("installationMethod");

		packMan = getPackman(installationMethod);
		siteMap.put("PackMan", packMan);
		packages = packMan.getListPackages();
		installedPackages = packMan.getListInstalledPackages();

		// Site name and CE name
		site = env.get("site");
		ce = env.get("CE");

		// TTL
		if (env.containsKey("TTL"))
			origTtl = Integer.parseInt(env.get("TTL"));
		else
			origTtl = 12 * 3600;

		siteMap.put("TTL", Integer.valueOf(origTtl));

		// CE Requirements
		if (env.containsKey("cerequirements"))
			ceRequirements = env.get("cerequirements");

		// Partition
		if (env.containsKey("partition"))
			partition = env.get("partition");

		// Close storage
		if (env.containsKey("closeSE"))
			extrasites = new ArrayList<>(Arrays.asList(env.get("closeSE").split(",")));

		// Get users from cerequirements field
		final ArrayList<String> users = new ArrayList<>();
		if (!ceRequirements.equals("")) {
			final Pattern p = Pattern.compile("\\s*other.user\\s*==\\s*\"(\\w+)\"");
			final Matcher m = p.matcher(ceRequirements);
			while (m.find())
				users.add(m.group(1));
		}

		// Get nousers from cerequirements field
		final ArrayList<String> nousers = new ArrayList<>();
		if (!ceRequirements.equals("")) {
			final Pattern p = Pattern.compile("\\s*other.user\\s*!=\\s*\"(\\w+)\"");
			final Matcher m = p.matcher(ceRequirements);
			while (m.find())
				nousers.add(m.group(1));
		}

		// We prepare the packages for direct matching
		String packs = ",";
		Collections.sort(packages);
		for (final String pack : packages)
			packs += pack + ",,";

		packs = packs.substring(0, packs.length() - 1);

		String instpacks = ",";
		Collections.sort(installedPackages);
		for (final String pack : installedPackages)
			instpacks += pack + ",,";

		instpacks = instpacks.substring(0, instpacks.length() - 1);

		// Workdir
		String workdir = System.getProperty("user.home");
		if (env.containsKey("WORKDIR"))
			workdir = env.get("WORKDIR");
		if (env.containsKey("TMPBATCH"))
			workdir = env.get("TMPBATCH");

		siteMap.put("workdir", workdir);

		// Setting values of the map
		siteMap.put("Platform", ConfigUtils.getPlatform());
		siteMap.put("Packages", packs);
		siteMap.put("InstalledPackages", instpacks);
		siteMap.put("CE", ce);
		siteMap.put("Site", site);
		if (users.size() > 0)
			siteMap.put("Users", users);
		if (nousers.size() > 0)
			siteMap.put("NoUsers", nousers);
		if (extrasites != null && extrasites.size() > 0)
			siteMap.put("Extrasites", extrasites);
		siteMap.put("Host", alienCm);

		if (env.containsKey("Disk"))
			siteMap.put("Disk", env.get("Disk"));
		else
			siteMap.put("Disk", Long.valueOf(new File(workdir).getFreeSpace() / 1024));

		if (!partition.equals(""))
			siteMap.put("Partition", partition);

		return siteMap;
	}

	// Gets a PackMan instance depending on configuration (env coming from LDAP)
	private PackMan getPackman(String installationMethod) {
		switch (installationMethod) {
		case "CVMFS":
			siteMap.put("CVMFS", Integer.valueOf(1));
			return new CVMFS();
		default:
			siteMap.put("CVMFS", Integer.valueOf(1));
			return new CVMFS();
		}
	}

}
