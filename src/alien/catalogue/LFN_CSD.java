package alien.catalogue;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.test.cassandra.DBCassandra;

/**
 * LFN implementation for Cassandra
 */
public class LFN_CSD implements Comparable<LFN_CSD>, CatalogEntity {
	/**
	 *
	 */
	private static final long serialVersionUID = 9158990164379160910L;

	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(LFN_CSD.class.getCanonicalName());

	/**
	 * Monitoring component
	 */
	static transient final Monitor monitor = MonitorFactory.getMonitor(LFN_CSD.class.getCanonicalName());

	/**
	 * Root UUID
	 */
	public static final UUID root_uuid = UUID.nameUUIDFromBytes("root".getBytes());

	/**
	 * Modulo of seNumber to partition balance
	 */
	public static final Integer modulo_se_lookup = 100;

	/**
	 * Owner
	 */
	public String owner;

	/**
	 * Last change timestamp
	 */
	public Date ctime;

	/**
	 * Size, in bytes
	 */
	public long size;

	/**
	 * Group
	 */
	public String gowner;

	/**
	 * File type
	 */
	public char type;

	/**
	 * Flag
	 */
	public Integer flag = 0;

	/**
	 * Access rights
	 */
	public String perm;

	/**
	 * MD5 checksum
	 */
	public String checksum;

	/**
	 * Whether or not this entry really exists in the catalogue
	 */
	public boolean exists = true;

	/**
	 * Canonical path
	 */
	public String canonicalName;

	/**
	 * Parent path (directory)
	 */
	public String path;

	/**
	 * Final part of path (name)
	 */
	public String child;

	/**
	 * id of the parent dir
	 */
	public UUID parent_id;

	/**
	 * id
	 */
	public UUID id;

	/**
	 * Job ID that produced this file
	 *
	 * @since AliEn 2.19
	 */
	public long jobid;

	/**
	 * physical/logical locations
	 */
	public HashMap<Integer, String> pfns = null;

	/**
	 * physical locations
	 */
	public HashMap<String, String> metadata = null;

	/**
	 * @param l
	 */
	public LFN_CSD(LFN l) {
		this(l, false);
	}

	/**
	 * @param LFN
	 * @param boolean
	 */
	public LFN_CSD(LFN l, boolean getParent) {
		canonicalName = l.getCanonicalName();

		String[] p_c = getPathAndChildFromCanonicalName(canonicalName);
		path = p_c[0];
		child = p_c[1];

		if (getParent) {
			parent_id = getParentIdFromPath(path);
			if (parent_id == null)
				return;
		}

		size = l.getSize();
		jobid = l.jobid;
		checksum = l.getMD5();
		type = l.getType();
		perm = l.getPermissions();
		ctime = l.ctime;
		owner = l.getOwner();
		gowner = l.getGroup();
		id = l.guid;
		flag = 0;
		metadata = new HashMap<String, String>();
	}

	public LFN_CSD(String lfn, boolean getFromDB, UUID p_id, UUID c_id) {
		canonicalName = lfn;

		String[] p_c = getPathAndChildFromCanonicalName(canonicalName);
		path = p_c[0];
		child = p_c[1];

		if (getFromDB) {
			try {
				parent_id = (p_id != null ? p_id : getParentIdFromPath(path));
				if (parent_id == null)
					return;

				id = (c_id != null ? c_id : getChildIdFromParentIdAndName(parent_id, child));
				if (id == null)
					return;

				final Session session = DBCassandra.getInstance();
				if (session == null)
					return;

				PreparedStatement statement = session.prepare("select checksum,ctime,gowner,jobid,metadata,owner,perm,pfns,size,type" + " from catalogue.lfn_metadata where parent_id = ? and id = ?");
				BoundStatement boundStatement = new BoundStatement(statement);
				boundStatement.bind(this.parent_id, this.id);

				boundStatement.setConsistencyLevel(ConsistencyLevel.QUORUM);

				ResultSet results = session.execute(boundStatement);
				init(results.one());
			} catch (Exception e) {
				System.err.println("Exception trying to create LFN_CSD: " + e);
				return;
			}
		}
	}

	public static String[] getPathAndChildFromCanonicalName(String canonicalName) {
		int remove = 0;
		int idx = canonicalName.lastIndexOf('/');
		if (idx == canonicalName.length() - 1) {
			idx = canonicalName.lastIndexOf('/', idx - 1);
			remove = 1;
		}
		String[] p_c = new String[2];
		p_c[0] = canonicalName.substring(0, idx + 1); // parent dir without trailing slash
		p_c[1] = canonicalName.substring(idx + 1, canonicalName.length() - remove); // last part of path without trailing slash
		return p_c;
	}

	/**
	 * @param row
	 */
	public LFN_CSD(Row row) {
		init(row);
	}

	private void init(Row row) {
		if (row == null) {
			logger.log(Level.SEVERE, "Row null creating LFN_CSD ");
			exists = false;
			return;
		}

		try {
			pfns = (HashMap<Integer, String>) row.getMap("pfns", Integer.class, String.class);
			metadata = (HashMap<String, String>) row.getMap("metadata", String.class, String.class);
			type = row.getString("type").charAt(0);
			checksum = row.getString("checksum");
			perm = row.getString("perm");
			jobid = row.getLong("jobid");
			size = row.getLong("size");
			ctime = row.getTimestamp("ctime");
			owner = row.getString("owner");
			gowner = row.getString("gowner");
			id = row.getUUID("id");
			if (type == 'd')
				canonicalName += "/";
		} catch (Exception e) {
			logger.log(Level.SEVERE, "Can't create LFN_CSD from row: " + e);
		}
	}

	/*
	 * Gets the id from the parent
	 * 
	 * @path_parent String representing the path/lfn of the parent
	 */
	public static UUID getParentIdFromPath(String path_parent) {
		try {
			UUID path_id = root_uuid;

			path_parent = path_parent.replaceAll("//", "/");
			if (path_parent.equals("/"))
				return path_id;

			path_parent = path_parent.replaceAll("^/", "");
			String[] path_chunks = path_parent.split("/");

			Session session = DBCassandra.getInstance();
			if (session == null)
				return null;

			// We loop from root until we reach our dir
			for (int i = 0; i < path_chunks.length + 1; i++) {
				PreparedStatement statement = session.prepare("select child_id from catalogue.lfn_index where path_id = ? and path = ?");
				BoundStatement boundStatement = new BoundStatement(statement);
				boundStatement.bind(path_id, (i == 0 ? "/" : path_chunks[i - 1]));
				boundStatement.setConsistencyLevel(ConsistencyLevel.QUORUM);
				ResultSet results = session.execute(boundStatement);
				if (results.getAvailableWithoutFetching() != 1)
					return null;

				path_id = results.one().getUUID("child_id");
				if (path_id == null)
					return null;
			}
			return path_id;
		} catch (Exception e) {
			System.err.println("Exception trying to getParentIdFromPath (" + path_parent + ") LFN_CSD: " + e);
			return null;
		}
	}

	/*
	 * Gets the id from the parent
	 * 
	 * @parent_id UUID representing the id of the parent in the index
	 * 
	 * @name String representing the path/lfn of the parent
	 */
	public static UUID getChildIdFromParentIdAndName(UUID parent_id, String name) {
		try {
			Session session = DBCassandra.getInstance();
			if (session == null)
				return null;

			PreparedStatement statement = session.prepare("select child_id from catalogue.lfn_index where path_id = ? and path = ?");
			BoundStatement boundStatement = new BoundStatement(statement);
			boundStatement.bind(parent_id, name);
			boundStatement.setConsistencyLevel(ConsistencyLevel.QUORUM);
			ResultSet results = session.execute(boundStatement);
			if (results.getAvailableWithoutFetching() != 1)
				return null;

			UUID id = results.one().getUUID("child_id");
			if (id == null)
				return null;

			return id;
		} catch (Exception e) {
			System.err.println("Exception trying to getChildIdFromParentIdAndName (" + parent_id + " ," + name + ") LFN_CSD: " + e);
			return null;
		}
	}

	@Override
	public String toString() {
		String str = "LFN (" + canonicalName + "): " + path + " " + child + "\n - Type: " + type;

		// if (type != 'd') {
		str += "\n - Size: " + size + "\n - Checksum: " + checksum + "\n - Perm: " + perm + "\n - Owner: " + owner + "\n - Gowner: " + gowner + "\n - JobId: " + jobid + "\n - Id: " + id
				+ "\n - Ctime: " + ctime;
		if (pfns != null)
			str += "\n - pfns: " + pfns.toString();
		if (metadata != null)
			str += "\n - metadata: " + metadata.toString();
		// }

		return str;
	}

	@Override
	public String getOwner() {
		return owner;
	}

	@Override
	public String getGroup() {
		return gowner;
	}

	@Override
	public String getPermissions() {
		return perm != null ? perm : "755";
	}

	@Override
	public String getName() {
		return canonicalName;
	}

	/**
	 * Get the canonical name (full path and name)
	 *
	 * @return canonical name
	 */
	public String getCanonicalName() {
		return canonicalName;
	}

	@Override
	public char getType() {
		return type;
	}

	@Override
	public long getSize() {
		return size;
	}

	@Override
	public String getMD5() {
		return checksum;
	}

	@Override
	public int compareTo(final LFN_CSD o) {
		if (this == o)
			return 0;

		return canonicalName.compareTo(o.canonicalName);
	}

	@Override
	public boolean equals(final Object obj) {
		if (obj == null || !(obj instanceof LFN_CSD))
			return false;

		if (this == obj)
			return true;

		final LFN_CSD other = (LFN_CSD) obj;

		return compareTo(other) == 0;
	}

	@Override
	public int hashCode() {
		return Integer.parseInt(perm) * 13 + canonicalName.hashCode() * 17;
	}

	/**
	 * is this LFN a directory
	 *
	 * @return <code>true</code> if this LFN is a directory
	 */
	public boolean isDirectory() {
		return (type == 'd');
	}

	/**
	 * @return <code>true</code> if this LFN points to a file
	 */
	public boolean isFile() {
		return (type == 'f' || type == '-');
	}

	/**
	 * @return <code>true</code> if this is a native collection
	 */
	public boolean isCollection() {
		return type == 'c';
	}

	/**
	 * @return <code>true</code> if this is a a member of an archive
	 */
	public boolean isMemberOfArchive() {
		return type == 'm';
	}

	/**
	 * @return <code>true</code> if this is an archive
	 */
	public boolean isArchive() {
		return type == 'a';
	}

	/**
	 * @return the list of entries in this folder
	 */

	public List<LFN_CSD> list() {
		return list(false, null, null);
	}

	public List<LFN_CSD> list(boolean get_metadata) {
		return list(get_metadata, null, null);
	}

	/**
	 * @param get_metadata
	 * @param append_table
	 * @param level
	 * @return list of LFNs from this table
	 */
	public List<LFN_CSD> list(boolean get_metadata, String append_table, ConsistencyLevel level) {
		if (!exists)
			return null;

		if (monitor != null)
			monitor.incrementCounter("LFN_CSD_list");

		final List<LFN_CSD> ret = new ArrayList<>();
		if (type != 'd') {
			ret.add(this);
			return ret;
		}

		String t = "catalogue.lfn_metadata";
		if (append_table != null) {
			t += append_table;
		}

		ConsistencyLevel cl = ConsistencyLevel.QUORUM;
		if (level != null)
			cl = level;

		try {
			@SuppressWarnings("resource")
			final Session session = DBCassandra.getInstance();
			if (session == null)
				return null;

			PreparedStatement statement = session.prepare("select path,child_id from " + t + " where parent_id = ?");
			BoundStatement boundStatement = new BoundStatement(statement);
			boundStatement.bind(this.id);

			boundStatement.setConsistencyLevel(cl);

			ResultSet results = session.execute(boundStatement);
			for (Row row : results) {
				// LFN_CSD String lfn, boolean getFromDB, UUID p_id, UUID c_id
				ret.add(new LFN_CSD(this.canonicalName + row.getString("path"), get_metadata, this.id, row.getUUID("child_id")));
			}
		} catch (Exception e) {
			System.err.println("Exception trying to whereis: " + e);
			return null;
		}

		return ret;
	}

	// /**
	// * @param base
	// * @param pattern
	// * @param parameters
	// * @param metadata
	// * @param table
	// * @param level
	// * @return LFNs that match
	// */
	// public static List<LFN_CSD> find(String base, String pattern, String parameters, String metadata) {
	// return find(base, pattern, parameters, metadata, null, null);
	// }
	//
	// public static List<LFN_CSD> find(String base, String pattern, String parameters, String metadata, String table, ConsistencyLevel level) {
	// if (monitor != null)
	// monitor.incrementCounter("LFN_CSD_find");
	//
	// final List<LFN_CSD> ret = new ArrayList<>();
	//
	// LFN_CSD baselfn = new LFN_CSD(base, true);
	// if (!baselfn.exists || baselfn.type != 'd')
	// return null;
	//
	// pattern = Format.replace(pattern, "*", ".*");
	// Pattern p = Pattern.compile(pattern);
	//
	// List<LFN_CSD> ls = baselfn.list();
	// List<LFN_CSD> new_entries = new ArrayList<>();
	//
	// boolean end_reached = ls.size() <= 0;
	// while (!end_reached) {
	// for (LFN_CSD l : ls) {
	//
	// if (l.type == 'd') {
	// new_entries.add(l);
	// continue;
	// }
	//
	// Matcher m = p.matcher(l.child);
	// if (m.find())
	// ret.add(l);
	// }
	// end_reached = new_entries.size() <= 0 || ret.size() >= 50;
	// ls.clear();
	// ls.addAll(new_entries);
	// new_entries.clear();
	// }
	//
	// return ret;
	// }

	/**
	 * @return physical locations of the file
	 */
	public HashMap<Integer, String> whereis() {
		return whereis(null, null);
	}

	/**
	 * @param table
	 * @param level
	 * @return physical locations of the file
	 */
	public HashMap<Integer, String> whereis(String append_table, ConsistencyLevel level) {
		if (!exists || type == 'd')
			return null;

		String t = "catalogue.lfn_metadata";
		if (append_table != null)
			t += append_table;

		ConsistencyLevel cl = ConsistencyLevel.QUORUM;
		if (level != null)
			cl = level;

		try {
			if (parent_id == null)
				parent_id = getParentIdFromPath(path);
			if (parent_id == null)
				return null;

			if (id == null)
				id = getChildIdFromParentIdAndName(parent_id, child);
			if (id == null)
				return null;

			@SuppressWarnings("resource")
			Session session = DBCassandra.getInstance();
			if (session == null)
				return null;

			PreparedStatement statement = session.prepare("select pfns from " + t + " where parent_id = ? and id = ?");
			BoundStatement boundStatement = new BoundStatement(statement);
			boundStatement.bind(this.parent_id, this.id);

			boundStatement.setConsistencyLevel(cl);

			ResultSet results = session.execute(boundStatement);
			for (Row row : results) {
				pfns = (HashMap<Integer, String>) row.getMap("pfns", Integer.class, String.class);
			}
		} catch (Exception e) {
			System.err.println("Exception trying to whereis: " + e);
			return null;
		}

		return pfns;
	}

	/**
	 * @return insertion result
	 */
	public boolean insert() {
		return this.insert(null, null);
	}

	/**
	 * @param table_lfns
	 * @param table_se_lookup
	 * @param level
	 * @return insertion result
	 */
	public boolean insert(String append_table, ConsistencyLevel level) {
		// lfn | ctime | dir | gowner | jobid | link | md5 | owner | perm | pfns
		// | size | type
		String tindex = "catalogue.lfn_index";
		if (append_table != null)
			tindex += append_table;

		String t = "catalogue.lfn_metadata";
		if (append_table != null)
			t += append_table;

		String ts = "catalogue.se_lookup";
		if (append_table != null)
			ts += append_table;

		ConsistencyLevel cl = ConsistencyLevel.QUORUM;
		if (level != null)
			cl = level;

		if (this.parent_id == null)
			this.parent_id = getParentIdFromPath(this.path);

		if (this.parent_id == null)
			return false;

		if (this.id == null)
			id = UUID.randomUUID();

		try {
			@SuppressWarnings("resource")
			final Session session = DBCassandra.getInstance();
			if (session == null)
				return false;

			PreparedStatement statement;
			BoundStatement boundStatement;

			// Insert the entry in the index
			statement = session.prepare("INSERT INTO " + tindex + " (path_id,path,ctime,child_id,flag)" + " VALUES (?,?,?,?,?)");
			boundStatement = new BoundStatement(statement);
			boundStatement.bind(parent_id, child, ctime, id, flag);
			boundStatement.setConsistencyLevel(cl);
			session.execute(boundStatement);

			// Insert the entry in the metadata
			if (type == 'a' || type == 'f' || type == 'm' || type == 'l') {
				statement = session.prepare("INSERT INTO " + t + " (parent_id, id, ctime, gowner, jobid, checksum, owner, perm, pfns, size, type, metadata)" + " VALUES (?,?,?,?,?,?,?,?,?,?,?,?)");
				boundStatement = new BoundStatement(statement);
				boundStatement.bind(parent_id, id, ctime, gowner, jobid, checksum, owner, perm, pfns, size, String.valueOf(type), metadata);
			}
			else { // 'd'
				statement = session.prepare("INSERT INTO " + t + " (parent_id, id, ctime, gowner, jobid, owner, perm, size, type)" + " VALUES (?,?,?,?,?,?,?,?,?)");
				boundStatement = new BoundStatement(statement);
				boundStatement.bind(parent_id, id, ctime, gowner, jobid, owner, perm, size, String.valueOf(type));
			}

			boundStatement.setConsistencyLevel(cl);
			session.execute(boundStatement);

			// Insert files and archives into se_lookup
			if (type == 'a' || type == 'f') {
				Set<Integer> seNumbers = pfns.keySet();
				for (Integer seNumber : seNumbers) {
					Integer modulo = seNumber % modulo_se_lookup;
					statement = session.prepare("INSERT INTO " + ts + " (seNumber, modulo, id, size, owner)" + " VALUES (?,?,?,?,?)");
					boundStatement = new BoundStatement(statement);
					boundStatement.bind(seNumber, modulo, id, size, owner);
					boundStatement.setConsistencyLevel(cl);
					session.execute(boundStatement);
				}
			}
		} catch (Exception e) {
			System.err.println("Exception trying to insert: " + e);
			// TODO: shall we try to delete here from the tables?
			return false;
		}

		return true;
	}

	/**
	 * @param folder
	 * @param table
	 * @param level
	 * @return <code>true</code> if the directory exists or was successfully created now
	 */
	public static boolean createDirectory(String folder, String table, ConsistencyLevel level) {
		// We want to create the whole hierarchy upstream
		// check if is already there
		if (folder.length() <= 1 || existsLfn(folder))
			return true;

		// get parent and create it if doesn't exist
		String[] p_c = getPathAndChildFromCanonicalName(folder);
		String path = p_c[0];

		createDirectory(path, table, level);

		LFN_CSD newdir = new LFN_CSD(folder, false, null, null); // ctime, gowner, jobid, link, md5, owner, perm, size, type)
		newdir.type = 'd';
		newdir.ctime = new Date();
		newdir.gowner = "aliprod";
		newdir.owner = "aliprod";
		newdir.jobid = 0l;
		newdir.checksum = "";
		newdir.perm = "755";
		newdir.size = 0;

		return newdir.insert(table, level);
	}

	/**
	 * @param lfn
	 * @param table
	 * @param level
	 * @return <code>true</code> if the LFN exists
	 */
	public static boolean existsLfn(String lfn) {
		String[] p_c = getPathAndChildFromCanonicalName(lfn);
		String parent_of_lfn = p_c[0];
		String child_of_lfn = p_c[1];

		UUID pid = getParentIdFromPath(parent_of_lfn);
		if (pid == null)
			return false;

		UUID idfolder = getChildIdFromParentIdAndName(pid, child_of_lfn);
		if (idfolder == null)
			return false;

		return true;
	}

}
