package alien.test.setup;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.Arrays;

import alien.test.TestBrain;
import alien.test.TestConfig;
import alien.test.utils.Functions;
import alien.test.utils.TestCommand;
import alien.test.utils.TestException;

/**
 * @author ron
 * @since October 8, 2011
 */
public class CreateDB {
	
	/**
	 * the SQL socket
	 */
	public static final String sql_socket = TestConfig.sql_home + "/jalien.mysql.sock";
	

	/**
	 * the SQL pid file
	 */
	public static final String sql_pid_file = "/tmp/jalien.mysql.pid";

	/**
	 * the SQL log file
	 */
	public static final String sql_log = TestConfig.sql_home + "/jalien.mysql.log";

	/**
	 * the SQL connection
	 */
	public static Connection cn;
	
	
	/**
	 * @return state of rampUp
	 * @throws Exception
	 */
	public static boolean rampUpDB() throws Exception{
		try{
		stopDatabase();
		} catch(Exception e){
			// ignore
			//e.printStackTrace();
		}
		initializeDatabase();
		startDatabase();

		Thread.sleep(2000); 
		connect();
		
		fillDatabase(mysql_passwd);
		fillDatabase(dataDB_content);
		fillDatabase(userDB_content);

		fillDatabase(catalogueInitialDirectories());

		return true;
	}
	

	/**
	 * @throws Exception 
	 */
	public static void stopDatabase() throws Exception{
		
		String sql_pid = Functions.getFileContent(sql_pid_file);
		
	
		TestCommand db = new TestCommand(new String[] {TestBrain.cKill,"-9",sql_pid});
		db.verbose();
		if(!db.exec()){
			throw new TestException("Could not stop LDAP: \n STDOUT: " + db.getStdOut()+"\n STDERR: " + db.getStdErr());
		}
	}
	
	
	private static void initializeDatabase() throws Exception{

		Functions.writeOutFile(my_cnf,
				my_cnf_content);
		
		final TestCommand db = new TestCommand(new String[] {  TestBrain.cMysqlInstallDB, "--datadir="+TestConfig.sql_home, 
				"--skip-name-resolve","--ldata="+TestConfig.sql_home});
		//db.verbose();
		if(!db.exec()){
			throw new TestException("Could not initialize MySQL, STDOUT: " + db.getStdOut()+"\n STDERR: " + db.getStdErr());
		}
		//System.out.println("MYSQL install DB STDOUT: " + db.getStdOut());
		//System.out.println("MYSQL install DB STDERR: " + db.getStdErr());
	}
	
	private static void startDatabase() throws Exception{
		final TestCommand db = new TestCommand(new String[] { TestBrain.cMysqldSafe, "--defaults-file="+my_cnf});
		db.daemonize();
		//db.verbose();
		if(!db.exec()){
			throw new TestException("Could not start MySQL, STDOUT: " + db.getStdOut()+"\n STDERR: " + db.getStdErr());
		}
		//System.out.println("MYSQLd safe STDOUT: " + db.getStdOut());
		//System.out.println("MYSQLd safe STDERR: " + db.getStdErr());
	}
	

	private static void connect() throws Exception{
		
		Class.forName( "com.mysql.jdbc.Driver" );
		cn = DriverManager.getConnection( "jdbc:mysql://127.0.0.1:"+TestConfig.sql_port+"/mysql", "root", "" );
	}	
	
	
	/**
	 * @param username
	 * @param uid
	 * @throws Exception 
	 */
	public static void addUserToDB(final String username, final String uid) throws Exception{
		
		fillDatabase(userAddIndexTable(username, uid));
	}
	
	/**
	 * @param seName
	 * @param seNumber 
	 * @param site 
	 * @param iodeamon
	 * @param storedir
	 * @param qos 
	 * @param freespace
	 * @throws Exception
	 */
	public static void addSEtoDB(final String seName, final String seNumber, final String site, final String iodeamon, 
			final String storedir, final String qos, final String freespace) throws Exception{

		String[] queries  = new String[]{
		"USE `"+ TestConfig.dataDB +"`;",
		"LOCK TABLES `SE` WRITE;",
		"INSERT INTO `SE` VALUES (" + seNumber + ",0,'','" + TestConfig.VO_name + "::" + site + "::" + seName +
			"','," + qos + ",','" + storedir + "','File',NULL,NULL,'','file://" + iodeamon + "','');",			
		"LOCK TABLES `SE_VOLUMES` WRITE;",
		"INSERT INTO `SE_VOLUMES` VALUES ('" + storedir + "',"+ seNumber +",0,'" + TestConfig.VO_name + "::" + site + "::" + seName +
				"','" +  storedir + "',-1,'file://" + iodeamon.substring(0,iodeamon.indexOf(':')) + "','"+ freespace + "');",
		"LOCK TABLES `SERanks` WRITE;",
		"INSERT INTO `SERanks` VALUES ('" + site + "','" + seNumber + "','0','0');",
		 "UNLOCK TABLES;"
		};
		
		fillDatabase(queries);
		
	}
	
	// (5,0,'','pcepalice12::CERN::OTHERSE',',tape,','/tmp/pcepalice12/log/OTHER_SE_DATA','File',NULL,NULL,'','file://localhost:8062','')
	// 'File',NULL,NULL,'','file://localhost.localdomain:8092','')
	
	
	private static void fillDatabase(final String[] queries) throws Exception{

		for (String query: Arrays.asList(queries))
			cn.createStatement().execute(query);	
				
	}
	
	

	private static String queryDB(final String query, final String column){
		
		try {
			ResultSet r = cn.createStatement().executeQuery(query);
			while (r.next()) 
				return r.getString("entryID");
		} catch (Exception e) {
			System.out.println("Error in SQL query: " + query);
			e.printStackTrace();
		}
		return "";
	}
	
	
	
	
	final static String[] mysql_passwd = {
		"update mysql.user set password=PASSWORD('"+TestConfig.sql_pass+"') where User='root';",
		"delete from mysql.user where user !='root';",
		//"insert into host VALUES(\"localhost\",\"%\",'Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y');",
		"GRANT ALL PRIVILEGES ON *.* TO root IDENTIFIED BY '"+TestConfig.sql_pass+"' WITH GRANT OPTION;",
		"GRANT ALL PRIVILEGES ON *.* TO root@localhost IDENTIFIED BY '"+TestConfig.sql_pass+"' WITH GRANT OPTION;",
		//"GRANT ALL PRIVILEGES ON *.* TO root@localhost IDENTIFIED BY '"+TestConfig.sql_pass+"' WITH GRANT OPTION;",
		"flush privileges;",
		
		//"create database if not exists alien_system;",
		//"create database if not exists processes;",
		//"create database if not exists transfers;",
		//"create database if not exists INFORMATIONSERVICE;",
		"create database if not exists ADMIN;",
	};
	
	
	
//	#AliEn Organisations
//
//	ALIEN_ORGANISATIONS="pcepalice12:3307"
//			Functions.writeOutFile(TestConfig.mysql_conf_file,
//					mysql_config_content());
//	
//	private static mysql_config_content(){
//		
//	}
	
	
	
	
//	CREATE DATABASE IF NOT EXISTS `ADMIN` DEFAULT CHARACTER SET latin1;
//	USE `ADMIN`;
//
//
//
//	LOCK TABLES `USERS_LDAP` WRITE;
//	INSERT INTO `USERS_LDAP` VALUES ('newuser',1,'/C=CH/O=AliEn/CN=test user cert'),('ali',1,'/C=CH/O=AliEn/CN=test user cert');
//	UNLOCK TABLES;
//
//	
//	DROP TABLE IF EXISTS `USERS_LDAP_ROLE`;
//	CREATE TABLE `USERS_LDAP_ROLE` (
//			`user` varchar(15) COLLATE latin1_general_cs NOT NULL,
//			`role` varchar(15) COLLATE latin1_general_cs DEFAULT NULL,
//			`up` smallint(6) DEFAULT NULL
//			) ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;
//
//	LOCK TABLES `USERS_LDAP_ROLE` WRITE;
//	INSERT INTO `USERS_LDAP_ROLE` VALUES ('ali','aliprod',1),('ali','admin',1);
//	UNLOCK TABLES;


	   
		final static String my_cnf = TestConfig.sql_home+"/my.cnf";

	   
	   final static String my_cnf_content =  

	   "[mysqld]\n"
	   + "user="+ System.getProperty("user.name")+"\n"
	   + "datadir="+TestConfig.sql_home+"\n"
	   + "port="+TestConfig.sql_port+"\n"
	   + "socket="+sql_socket+"\n"
	   + "\n"
	   + "[mysqld_safe]\n"
	   + "log-error="+sql_log+"\n"
	   + "pid-file="+sql_pid_file+"\n"
	   + "\n"
	   + "[client]\n"
	   + "port="+TestConfig.sql_port+"\n"
	   + "user="+ System.getProperty("user.name")+"\n"
	   + "socket="+sql_socket+"\n"
	   + "\n"
	   + "[mysqladmin]\n"
	   + "user=root\n"
	   + "port="+TestConfig.sql_port+"\n"
	   + "socket="+sql_socket+"\n"
	   + "\n"
	   + "[mysql]\n"
	   + "port="+TestConfig.sql_port+"\n"
	   + "socket="+sql_socket+"\n"
	   + "\n"
	   + "[mysql_install_db]\n"
	   + "user="+ System.getProperty("user.name")+"\n"
	   + "port="+TestConfig.sql_port+"\n"
	   + "datadir="+TestConfig.sql_home+"\n"
	   + "socket="+sql_socket+"\n"
	   + "\n"
	   + "\n";

	

		
		
		static final String[] dataDB_content = {
		   "CREATE DATABASE IF NOT EXISTS `"+ TestConfig.dataDB +"` DEFAULT CHARACTER SET latin1;",
		   "USE `"+ TestConfig.dataDB +"`;",

		   "DROP TABLE IF EXISTS `ACL`;",
		   "CREATE TABLE `ACL` ("
		   + "  `entryId` int(11) NOT NULL AUTO_INCREMENT,"
		   + "  `owner` char(10) COLLATE latin1_general_cs NOT NULL,"
		   + "  `aclId` int(11) NOT NULL,"
		   + "  `perm` char(4) COLLATE latin1_general_cs NOT NULL,"
		   + "  PRIMARY KEY (`entryId`)"
		   + ") ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

		   "DROP TABLE IF EXISTS `ACTIONS`;",
		   "CREATE TABLE `ACTIONS` ("
		   + "  `action` char(40) COLLATE latin1_general_cs NOT NULL,"
		   + "  `todo` int(1) NOT NULL DEFAULT '0',"
		   + "  PRIMARY KEY (`action`)"
		   + ") ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

		   "DROP TABLE IF EXISTS `COLLECTIONS`;",
		   "CREATE TABLE `COLLECTIONS` ("
		   + "  `collectionId` int(11) NOT NULL AUTO_INCREMENT,"
		   + "  `collGUID` binary(16) DEFAULT NULL,"
		   + "  PRIMARY KEY (`collectionId`)"
		   + ") ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

		   "DROP TABLE IF EXISTS `COLLECTIONS_ELEM`;",
		   "CREATE TABLE `COLLECTIONS_ELEM` ("
		   + "  `collectionId` int(11) NOT NULL,"
		   + "  `localName` varchar(255) COLLATE latin1_general_cs DEFAULT NULL,"
		   + "  `data` varchar(255) COLLATE latin1_general_cs DEFAULT NULL,"
		   + "  `origLFN` varchar(255) COLLATE latin1_general_cs DEFAULT NULL,"
		   + "  `guid` binary(16) DEFAULT NULL,"
		   + "  KEY `collectionId` (`collectionId`)"
		   + ") ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

		   "DROP TABLE IF EXISTS `CONSTANTS`;",
		   "CREATE TABLE `CONSTANTS` ("
		   + "  `name` varchar(100) COLLATE latin1_general_cs NOT NULL,"
		   + "  `value` int(11) DEFAULT NULL,"
		   + "  PRIMARY KEY (`name`)"
		   + ") ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

		   "DROP TABLE IF EXISTS `ENVIRONMENT`;",
		   "CREATE TABLE `ENVIRONMENT` ("
		   + "  `userName` char(20) COLLATE latin1_general_cs NOT NULL,"
		   + "  `env` char(255) COLLATE latin1_general_cs DEFAULT NULL,"
		   + "  PRIMARY KEY (`userName`)"
		   + ") ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

		   "DROP TABLE IF EXISTS `FQUOTAS`;",
		   "CREATE TABLE `FQUOTAS` ("
		   + "  `user` varchar(64) COLLATE latin1_general_cs NOT NULL,"
		   + "  `maxNbFiles` int(11) NOT NULL DEFAULT '0',"
		   + "  `nbFiles` int(11) NOT NULL DEFAULT '0',"
		   + "  `tmpIncreasedTotalSize` bigint(20) NOT NULL DEFAULT '0',"
		   + "  `maxTotalSize` bigint(20) NOT NULL DEFAULT '0',"
		   + "  `tmpIncreasedNbFiles` int(11) NOT NULL DEFAULT '0',"
		   + "  `totalSize` bigint(20) NOT NULL DEFAULT '0',"
		   + "  PRIMARY KEY (`user`)"
		   + ") ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

		   "DROP TABLE IF EXISTS `G0L`;",
		   "CREATE TABLE `G0L` ("
		   + "  `guidId` int(11) NOT NULL AUTO_INCREMENT,"
		   + "  `ctime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,"
		   + "  `owner` varchar(20) COLLATE latin1_general_cs DEFAULT NULL,"
		   + "  `ref` int(11) DEFAULT '0',"
		   + "  `jobid` int(11) DEFAULT NULL,"
		   + "  `seStringlist` varchar(255) COLLATE latin1_general_cs NOT NULL DEFAULT ',',"
		   + "  `seAutoStringlist` varchar(255) COLLATE latin1_general_cs NOT NULL DEFAULT ',',"
		   + "  `aclId` int(11) DEFAULT NULL,"
		   + "  `expiretime` datetime DEFAULT NULL,"
		   + "  `size` bigint(20) NOT NULL DEFAULT '0',"
		   + "  `gowner` varchar(20) COLLATE latin1_general_cs DEFAULT NULL,"
		   + "  `guid` binary(16) DEFAULT NULL,"
		   + "  `type` char(1) COLLATE latin1_general_cs DEFAULT NULL,"
		   + "  `md5` varchar(32) COLLATE latin1_general_cs DEFAULT NULL,"
		   + "  `perm` char(3) COLLATE latin1_general_cs DEFAULT NULL,"
		   + "  PRIMARY KEY (`guidId`),"
		   + "  UNIQUE KEY `guid` (`guid`),"
		   + "  KEY `seStringlist` (`seStringlist`),"
		   + "  KEY `ctime` (`ctime`)"
		   + ") ENGINE=MyISAM AUTO_INCREMENT=34 DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

		   "DROP TABLE IF EXISTS `G0L_PFN`;",
		   "CREATE TABLE `G0L_PFN` ("
		   + "  `guidId` int(11) NOT NULL,"
		   + "  `pfn` varchar(255) COLLATE latin1_general_cs DEFAULT NULL,"
		   + "  `seNumber` int(11) NOT NULL,"
		   + "  KEY `guid_ind` (`guidId`),"
		   + "  KEY `seNumber` (`seNumber`)"
		   + ") ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

		   "DROP TABLE IF EXISTS `G0L_QUOTA`;",
		   "CREATE TABLE `G0L_QUOTA` ("
		   + "  `user` varchar(64) COLLATE latin1_general_cs NOT NULL,"
		   + "  `nbFiles` int(11) NOT NULL,"
		   + "  `totalSize` bigint(20) NOT NULL,"
		   + "  KEY `user_ind` (`user`)"
		   + ") ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

		   "DROP TABLE IF EXISTS `G0L_REF`;",
		   "CREATE TABLE `G0L_REF` ("
		   + "  `guidId` int(11) NOT NULL,"
		   + "  `lfnRef` varchar(20) COLLATE latin1_general_cs NOT NULL,"
		   + "  KEY `guidId` (`guidId`),"
		   + "  KEY `lfnRef` (`lfnRef`)"
		   + ") ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

		   "DROP TABLE IF EXISTS `GL_ACTIONS`;",
		   "CREATE TABLE `GL_ACTIONS` ("
		   + "  `tableNumber` int(11) NOT NULL,"
		   + "  `time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,"
		   + "  `action` char(40) COLLATE latin1_general_cs NOT NULL,"
		   + "  `extra` varchar(255) COLLATE latin1_general_cs DEFAULT NULL,"
		   + "  UNIQUE KEY `tableNumber` (`tableNumber`,`action`)"
		   + ") ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

		   "DROP TABLE IF EXISTS `GL_STATS`;",
		   "CREATE TABLE `GL_STATS` ("
		   + "  `tableNumber` int(11) NOT NULL,"
		   + "  `seNumFiles` bigint(20) DEFAULT NULL,"
		   + "  `seNumber` int(11) NOT NULL,"
		   + "  `seUsedSpace` bigint(20) DEFAULT NULL,"
		   + "  UNIQUE KEY `tableNumber` (`tableNumber`,`seNumber`)"
		   + ") ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

		   "DROP TABLE IF EXISTS `GROUPS`;",
		   "CREATE TABLE `GROUPS` ("
		   + "  `Userid` int(11) NOT NULL AUTO_INCREMENT,"
		   + "  `PrimaryGroup` int(1) DEFAULT NULL,"
		   + "  `Groupname` char(85) COLLATE latin1_general_cs DEFAULT NULL,"
		   + "  `Username` char(20) COLLATE latin1_general_cs NOT NULL,"
		   + "  PRIMARY KEY (`Userid`)"
		   + ") ENGINE=MyISAM AUTO_INCREMENT=5 DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

		   "DROP TABLE IF EXISTS `GUIDINDEX`;",
		   "CREATE TABLE `GUIDINDEX` ("
		   + "  `tableName` int(11) NOT NULL,"
		   + "  `guidTime` varchar(16) COLLATE latin1_general_cs DEFAULT '0',"
		   + "  PRIMARY KEY (`tableName`),"
		   + "  UNIQUE KEY `guidTime` (`guidTime`)"
		   + ") ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

		   "DROP TABLE IF EXISTS `INDEXTABLE`;",
		   "CREATE TABLE `INDEXTABLE` ("
		   + "  `indexId` int(11) NOT NULL AUTO_INCREMENT,"
		   + "  `hostIndex` int(11) NOT NULL,"
		   + "  `tableName` int(11) NOT NULL,"
		   + "  `lfn` varchar(255) COLLATE latin1_general_cs DEFAULT NULL,"
		   + "  PRIMARY KEY (`indexId`),"
		   + "  UNIQUE KEY `lfn` (`lfn`)"
		   + ") ENGINE=MyISAM AUTO_INCREMENT=2 DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

		   "DROP TABLE IF EXISTS `L0L`;",
		   "CREATE TABLE `L0L` ("
		   + "  `entryId` bigint(11) NOT NULL AUTO_INCREMENT,"
		   + "  `owner` varchar(20) COLLATE latin1_general_cs NOT NULL,"
		   + "  `replicated` smallint(1) NOT NULL DEFAULT '0',"
		   + "  `ctime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,"
		   + "  `guidtime` varchar(8) COLLATE latin1_general_cs DEFAULT NULL,"
		   + "  `jobid` int(11) DEFAULT NULL,"
		   + "  `aclId` mediumint(11) DEFAULT NULL,"
		   + "  `lfn` varchar(255) COLLATE latin1_general_cs DEFAULT NULL,"
		   + "  `broken` smallint(1) NOT NULL DEFAULT '0',"
		   + "  `expiretime` datetime DEFAULT NULL,"
		   + "  `size` bigint(20) NOT NULL DEFAULT '0',"
		   + "  `dir` bigint(11) DEFAULT NULL,"
		   + "  `gowner` varchar(20) COLLATE latin1_general_cs NOT NULL,"
		   + "  `type` char(1) COLLATE latin1_general_cs NOT NULL DEFAULT 'f',"
		   + "  `guid` binary(16) DEFAULT NULL,"
		   + "  `md5` varchar(32) COLLATE latin1_general_cs DEFAULT NULL,"
		   + "  `perm` char(3) COLLATE latin1_general_cs NOT NULL,"
		   + "  PRIMARY KEY (`entryId`),"
		   + "  UNIQUE KEY `lfn` (`lfn`),"
		   + "  KEY `dir` (`dir`),"
		   + "  KEY `guid` (`guid`),"
		   + "  KEY `type` (`type`),"
		   + "  KEY `ctime` (`ctime`),"
		   + "  KEY `guidtime` (`guidtime`)"
		   + ") ENGINE=MyISAM AUTO_INCREMENT=42 DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

		   "DROP TABLE IF EXISTS `L0L_QUOTA`;",
		   "CREATE TABLE `L0L_QUOTA` ("
		   + "  `user` varchar(64) COLLATE latin1_general_cs NOT NULL,"
		   + "  `nbFiles` int(11) NOT NULL,"
		   + "  `totalSize` bigint(20) NOT NULL,"
		   + "  KEY `user_ind` (`user`)"
		   + ") ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

		   "DROP TABLE IF EXISTS `L0L_broken`;",
		   "CREATE TABLE `L0L_broken` ("
		   + "  `entryId` bigint(11) NOT NULL,"
		   + "  PRIMARY KEY (`entryId`)"
		   + ") ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

		   "DROP TABLE IF EXISTS `LFN_BOOKED`;",
		   "CREATE TABLE `LFN_BOOKED` ("
		   + "  `lfn` varchar(255) COLLATE latin1_general_cs NOT NULL DEFAULT '',"
		   + "  `owner` varchar(20) CHARACTER SET latin1 COLLATE latin1_general_ci DEFAULT NULL,"
		   + "  `quotaCalculated` smallint(6) DEFAULT NULL,"
		   + "  `existing` smallint(1) DEFAULT NULL,"
		   + "  `jobid` int(11) DEFAULT NULL,"
		   + "  `md5sum` varchar(32) COLLATE latin1_general_cs DEFAULT NULL,"
		   + "  `expiretime` int(11) DEFAULT NULL,"
		   + "  `size` bigint(20) DEFAULT NULL,"
		   + "  `pfn` varchar(255) COLLATE latin1_general_cs NOT NULL DEFAULT '',"
		   + "  `se` varchar(100) CHARACTER SET latin1 COLLATE latin1_general_ci DEFAULT NULL,"
		   + "  `gowner` varchar(20) COLLATE latin1_general_cs DEFAULT NULL,"
		   + "  `user` varchar(20) CHARACTER SET latin1 COLLATE latin1_general_ci DEFAULT NULL,"
		   + "  `guid` binary(16) NOT NULL DEFAULT '\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0',"
		   + "  PRIMARY KEY (`lfn`,`pfn`,`guid`),"
		   + "  KEY `pfn` (`pfn`),"
		   + "  KEY `guid` (`guid`),"
		   + "  KEY `jobid` (`jobid`)"
		   + ") ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

		   "DROP TABLE IF EXISTS `LFN_UPDATES`;",
		   "CREATE TABLE `LFN_UPDATES` ("
		   + "  `guid` binary(16) DEFAULT NULL,"
		   + "  `entryId` int(11) NOT NULL AUTO_INCREMENT,"
		   + "  `action` char(10) COLLATE latin1_general_cs DEFAULT NULL,"
		   + "  PRIMARY KEY (`entryId`),"
		   + "  KEY `guid` (`guid`)"
		   + ") ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

		   "DROP TABLE IF EXISTS `LL_ACTIONS`;",
		   "CREATE TABLE `LL_ACTIONS` ("
		   + "  `tableNumber` int(11) NOT NULL,"
		   + "  `time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,"
		   + "  `action` char(40) COLLATE latin1_general_cs NOT NULL,"
		   + "  `extra` varchar(255) COLLATE latin1_general_cs DEFAULT NULL,"
		   + "  UNIQUE KEY `tableNumber` (`tableNumber`,`action`)"
		   + ") ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

		   "DROP TABLE IF EXISTS `LL_STATS`;",
		   "CREATE TABLE `LL_STATS` ("
		   + "  `tableNumber` int(11) NOT NULL,"
		   + "  `max_time` char(20) COLLATE latin1_general_cs NOT NULL,"
		   + "  `min_time` char(20) COLLATE latin1_general_cs NOT NULL,"
		   + "  UNIQUE KEY `tableNumber` (`tableNumber`)"
		   + ") ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

		   "DROP TABLE IF EXISTS `PACKAGES`;",
		   "CREATE TABLE `PACKAGES` ("
		   + "  `fullPackageName` varchar(255) COLLATE latin1_general_cs DEFAULT NULL,"
		   + "  `lfn` varchar(255) COLLATE latin1_general_cs DEFAULT NULL,"
		   + "  `packageName` varchar(255) COLLATE latin1_general_cs DEFAULT NULL,"
		   + "  `username` varchar(20) COLLATE latin1_general_cs DEFAULT NULL,"
		   + "  `size` bigint(20) DEFAULT NULL,"
		   + "  `platform` varchar(255) COLLATE latin1_general_cs DEFAULT NULL,"
		   + "  `packageVersion` varchar(255) COLLATE latin1_general_cs DEFAULT NULL"
		   + ") ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

		   "DROP TABLE IF EXISTS `SE`;",
		   "CREATE TABLE `SE` ("
		   + "  `seNumber` int(11) NOT NULL AUTO_INCREMENT,"
		   + "  `seMinSize` int(11) DEFAULT '0',"
		   + "  `seExclusiveWrite` varchar(300) CHARACTER SET latin1 COLLATE latin1_general_ci DEFAULT NULL,"
		   + "  `seName` varchar(60) CHARACTER SET latin1 COLLATE latin1_general_ci NOT NULL,"
		   + "  `seQoS` varchar(200) CHARACTER SET latin1 COLLATE latin1_general_ci DEFAULT NULL,"
		   + "  `seStoragePath` varchar(255) COLLATE latin1_general_cs DEFAULT NULL,"
		   + "  `seType` varchar(60) COLLATE latin1_general_cs DEFAULT NULL,"
		   + "  `seNumFiles` bigint(20) DEFAULT NULL,"
		   + "  `seUsedSpace` bigint(20) DEFAULT NULL,"
		   + "  `seExclusiveRead` varchar(300) CHARACTER SET latin1 COLLATE latin1_general_ci DEFAULT NULL,"
		   + "  `seioDaemons` varchar(255) COLLATE latin1_general_cs DEFAULT NULL,"
		   + "  `seVersion` varchar(300) COLLATE latin1_general_cs DEFAULT NULL,"
		   + "  PRIMARY KEY (`seNumber`),"
		   + "  UNIQUE KEY `seName` (`seName`)"
		   + ") ENGINE=MyISAM AUTO_INCREMENT=6 DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

		   "DROP TABLE IF EXISTS `SERanks`;",
		   "CREATE TABLE `SERanks` ("
		   + "  `sitename` varchar(100) CHARACTER SET latin1 COLLATE latin1_general_ci NOT NULL,"
		   + "  `seNumber` int(11) NOT NULL,"
		   + "  `updated` smallint(1) DEFAULT NULL,"
		   + "  `rank` smallint(7) NOT NULL"
		   + ") ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

		   "DROP TABLE IF EXISTS `SE_VOLUMES`;",
		   "CREATE TABLE `SE_VOLUMES` ("
		   + "  `volume` char(255) COLLATE latin1_general_cs NOT NULL,"
		   + "  `volumeId` int(11) NOT NULL AUTO_INCREMENT,"
		   + "  `usedspace` bigint(20) DEFAULT NULL,"
		   + "  `seName` char(255) CHARACTER SET latin1 COLLATE latin1_general_ci NOT NULL,"
		   + "  `mountpoint` char(255) COLLATE latin1_general_cs DEFAULT NULL,"
		   + "  `size` bigint(20) DEFAULT NULL,"
		   + "  `method` char(255) COLLATE latin1_general_cs DEFAULT NULL,"
		   + "  `freespace` bigint(20) DEFAULT NULL,"
		   + "  PRIMARY KEY (`volumeId`),"
		   + "  KEY `seName` (`seName`),"
		   + "  KEY `volume` (`volume`)"
		   + ") ENGINE=MyISAM AUTO_INCREMENT=5 DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

		   "DROP TABLE IF EXISTS `HOSTS`;",
		   "CREATE TABLE `HOSTS` ("
		   + "  `hostIndex` int(11) NOT NULL AUTO_INCREMENT,"
		   + "  `address` char(50) COLLATE latin1_general_cs,"
		   + "  `db` char(40) COLLATE latin1_general_cs,"
		   + "  `driver` char(10) COLLATE latin1_general_cs,"
		   + "  `organisation` char(40) COLLATE latin1_general_cs,"
	       + "  PRIMARY KEY (`hostIndex`)"
 		   + ") ENGINE=MyISAM AUTO_INCREMENT=2 DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

		   
		   
			};

		

		static final String[] userDB_content = {
		   "CREATE DATABASE IF NOT EXISTS `"+ TestConfig.userDB +"` DEFAULT CHARACTER SET latin1;",
		   "USE `"+ TestConfig.userDB +"`;",

		   "DROP TABLE IF EXISTS `ACL`;",
		   "CREATE TABLE `ACL` ("
		   + "  `entryId` int(11) NOT NULL AUTO_INCREMENT,"
		   + "  `owner` char(10) COLLATE latin1_general_cs NOT NULL,"
		   + "  `aclId` int(11) NOT NULL,"
		   + "  `perm` char(4) COLLATE latin1_general_cs NOT NULL,"
		   + "  PRIMARY KEY (`entryId`)"
		   + ") ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

		   "DROP TABLE IF EXISTS `ACTIONS`;",
		   "CREATE TABLE `ACTIONS` ("
		   + "  `action` char(40) COLLATE latin1_general_cs NOT NULL,"
		   + "  `todo` int(1) NOT NULL DEFAULT '0',"
		   + "  PRIMARY KEY (`action`)"
		   + ") ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

		   "DROP TABLE IF EXISTS `COLLECTIONS`;",
		   "CREATE TABLE `COLLECTIONS` ("
		   + "  `collectionId` int(11) NOT NULL AUTO_INCREMENT,"
		   + "  `collGUID` binary(16) DEFAULT NULL,"
		   + "  PRIMARY KEY (`collectionId`)"
		   + ") ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

		   "DROP TABLE IF EXISTS `COLLECTIONS_ELEM`;",
		   "CREATE TABLE `COLLECTIONS_ELEM` ("
		   + "  `collectionId` int(11) NOT NULL,"
		   + "  `localName` varchar(255) COLLATE latin1_general_cs DEFAULT NULL,"
		   + "  `data` varchar(255) COLLATE latin1_general_cs DEFAULT NULL,"
		   + "  `origLFN` varchar(255) COLLATE latin1_general_cs DEFAULT NULL,"
		   + "  `guid` binary(16) DEFAULT NULL,"
		   + "  KEY `collectionId` (`collectionId`)"
		   + ") ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

		   "DROP TABLE IF EXISTS `CONSTANTS`;",
		   "CREATE TABLE `CONSTANTS` ("
		   + "  `name` varchar(100) COLLATE latin1_general_cs NOT NULL,"
		   + "  `value` int(11) DEFAULT NULL,"
		   + "  PRIMARY KEY (`name`)"
		   + ") ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

		   "DROP TABLE IF EXISTS `ENVIRONMENT`;",
		   "CREATE TABLE `ENVIRONMENT` ("
		   + "  `userName` char(20) COLLATE latin1_general_cs NOT NULL,"
		   + "  `env` char(255) COLLATE latin1_general_cs DEFAULT NULL,"
		   + "  PRIMARY KEY (`userName`)"
		   + ") ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

		   "DROP TABLE IF EXISTS `FQUOTAS`;",
		   "CREATE TABLE `FQUOTAS` ("
		   + "  `user` varchar(64) COLLATE latin1_general_cs NOT NULL,"
		   + "  `maxNbFiles` int(11) NOT NULL DEFAULT '0',"
		   + "  `nbFiles` int(11) NOT NULL DEFAULT '0',"
		   + "  `tmpIncreasedTotalSize` bigint(20) NOT NULL DEFAULT '0',"
		   + "  `maxTotalSize` bigint(20) NOT NULL DEFAULT '0',"
		   + "  `tmpIncreasedNbFiles` int(11) NOT NULL DEFAULT '0',"
		   + "  `totalSize` bigint(20) NOT NULL DEFAULT '0',"
		   + "  PRIMARY KEY (`user`)"
		   + ") ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

		   "DROP TABLE IF EXISTS `G0L`;",
		   "CREATE TABLE `G0L` ("
		   + "  `guidId` int(11) NOT NULL AUTO_INCREMENT,"
		   + "  `ctime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,"
		   + "  `owner` varchar(20) COLLATE latin1_general_cs DEFAULT NULL,"
		   + "  `ref` int(11) DEFAULT '0',"
		   + "  `jobid` int(11) DEFAULT NULL,"
		   + "  `seStringlist` varchar(255) COLLATE latin1_general_cs NOT NULL DEFAULT ',',"
		   + "  `seAutoStringlist` varchar(255) COLLATE latin1_general_cs NOT NULL DEFAULT ',',"
		   + "  `aclId` int(11) DEFAULT NULL,"
		   + "  `expiretime` datetime DEFAULT NULL,"
		   + "  `size` bigint(20) NOT NULL DEFAULT '0',"
		   + "  `gowner` varchar(20) COLLATE latin1_general_cs DEFAULT NULL,"
		   + "  `guid` binary(16) DEFAULT NULL,"
		   + "  `type` char(1) COLLATE latin1_general_cs DEFAULT NULL,"
		   + "  `md5` varchar(32) COLLATE latin1_general_cs DEFAULT NULL,"
		   + "  `perm` char(3) COLLATE latin1_general_cs DEFAULT NULL,"
		   + "  PRIMARY KEY (`guidId`),"
		   + "  UNIQUE KEY `guid` (`guid`),"
		   + "  KEY `seStringlist` (`seStringlist`),"
		   + "  KEY `ctime` (`ctime`)"
		   + ") ENGINE=MyISAM AUTO_INCREMENT=34 DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

		   "DROP TABLE IF EXISTS `G0L_PFN`;",
		   "CREATE TABLE `G0L_PFN` ("
		   + "  `guidId` int(11) NOT NULL,"
		   + "  `pfn` varchar(255) COLLATE latin1_general_cs DEFAULT NULL,"
		   + "  `seNumber` int(11) NOT NULL,"
		   + "  KEY `guid_ind` (`guidId`),"
		   + "  KEY `seNumber` (`seNumber`)"
		   + ") ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

		   "DROP TABLE IF EXISTS `G0L_QUOTA`;",
		   "CREATE TABLE `G0L_QUOTA` ("
		   + "  `user` varchar(64) COLLATE latin1_general_cs NOT NULL,"
		   + "  `nbFiles` int(11) NOT NULL,"
		   + "  `totalSize` bigint(20) NOT NULL,"
		   + "  KEY `user_ind` (`user`)"
		   + ") ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",
		   
		   "DROP TABLE IF EXISTS `G0L_REF`;",
		   "CREATE TABLE `G0L_REF` ("
		   + "  `guidId` int(11) NOT NULL,"
		   + "  `lfnRef` varchar(20) COLLATE latin1_general_cs NOT NULL,"
		   + "  KEY `guidId` (`guidId`),"
		   + "  KEY `lfnRef` (`lfnRef`)"
		   + ") ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

		   "DROP TABLE IF EXISTS `GL_ACTIONS`;",
		   "CREATE TABLE `GL_ACTIONS` ("
		   + "  `tableNumber` int(11) NOT NULL,"
		   + "  `time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,"
		   + "  `action` char(40) COLLATE latin1_general_cs NOT NULL,"
		   + "  `extra` varchar(255) COLLATE latin1_general_cs DEFAULT NULL,"
		   + "  UNIQUE KEY `tableNumber` (`tableNumber`,`action`)"
		   + ") ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

		   "DROP TABLE IF EXISTS `GL_STATS`;",
		   "CREATE TABLE `GL_STATS` ("
		   + "  `tableNumber` int(11) NOT NULL,"
		   + "  `seNumFiles` bigint(20) DEFAULT NULL,"
		   + "  `seNumber` int(11) NOT NULL,"
		   + "  `seUsedSpace` bigint(20) DEFAULT NULL,"
		   + "  UNIQUE KEY `tableNumber` (`tableNumber`,`seNumber`)"
		   + ") ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

		   "DROP TABLE IF EXISTS `GROUPS`;",
		   "CREATE TABLE `GROUPS` ("
		   + "  `Userid` int(11) NOT NULL AUTO_INCREMENT,"
		   + "  `PrimaryGroup` int(1) DEFAULT NULL,"
		   + "  `Groupname` char(85) COLLATE latin1_general_cs DEFAULT NULL,"
		   + "  `Username` char(20) COLLATE latin1_general_cs NOT NULL,"
		   + "  PRIMARY KEY (`Userid`)"
		   + ") ENGINE=MyISAM AUTO_INCREMENT=5 DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

		   "DROP TABLE IF EXISTS `GUIDINDEX`;",
		   "CREATE TABLE `GUIDINDEX` ("
		   + "  `tableName` int(11) NOT NULL,"
		   + "  `guidTime` varchar(16) COLLATE latin1_general_cs DEFAULT '0',"
		   + "  PRIMARY KEY (`tableName`),"
		   + "  UNIQUE KEY `guidTime` (`guidTime`)"
		   + ") ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

		   "DROP TABLE IF EXISTS `INDEXTABLE`;",
		   "CREATE TABLE `INDEXTABLE` ("
		   + "  `indexId` int(11) NOT NULL AUTO_INCREMENT,"
		   + "  `hostIndex` int(11) NOT NULL,"
		   + "  `tableName` int(11) NOT NULL,"
		   + "  `lfn` varchar(255) COLLATE latin1_general_cs DEFAULT NULL,"
		   + "  PRIMARY KEY (`indexId`),"
		   + "  UNIQUE KEY `lfn` (`lfn`)"
		   + ") ENGINE=MyISAM AUTO_INCREMENT=2 DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

		   "DROP TABLE IF EXISTS `L0L`;",
		   "CREATE TABLE `L0L` ("
		   + "  `entryId` bigint(11) NOT NULL AUTO_INCREMENT,"
		   + "  `owner` varchar(20) COLLATE latin1_general_cs NOT NULL,"
		   + "  `replicated` smallint(1) NOT NULL DEFAULT '0',"
		   + "  `ctime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,"
		   + "  `guidtime` varchar(8) COLLATE latin1_general_cs DEFAULT NULL,"
		   + "  `jobid` int(11) DEFAULT NULL,"
		   + "  `aclId` mediumint(11) DEFAULT NULL,"
		   + "  `lfn` varchar(255) COLLATE latin1_general_cs DEFAULT NULL,"
		   + "  `broken` smallint(1) NOT NULL DEFAULT '0',"
		   + "  `expiretime` datetime DEFAULT NULL,"
		   + "  `size` bigint(20) NOT NULL DEFAULT '0',"
		   + "  `dir` bigint(11) DEFAULT NULL,"
		   + "  `gowner` varchar(20) COLLATE latin1_general_cs NOT NULL,"
		   + "  `type` char(1) COLLATE latin1_general_cs NOT NULL DEFAULT 'f',"
		   + "  `guid` binary(16) DEFAULT NULL,"
		   + "  `md5` varchar(32) COLLATE latin1_general_cs DEFAULT NULL,"
		   + "  `perm` char(3) COLLATE latin1_general_cs NOT NULL,"
		   + "  PRIMARY KEY (`entryId`),"
		   + "  UNIQUE KEY `lfn` (`lfn`),"
		   + "  KEY `dir` (`dir`),"
		   + "  KEY `guid` (`guid`),"
		   + "  KEY `type` (`type`),"
		   + "  KEY `ctime` (`ctime`),"
		   + "  KEY `guidtime` (`guidtime`)"
		   + ") ENGINE=MyISAM AUTO_INCREMENT=42 DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

		   "DROP TABLE IF EXISTS `L0L_QUOTA`;",
		   "CREATE TABLE `L0L_QUOTA` ("
		   + "  `user` varchar(64) COLLATE latin1_general_cs NOT NULL,"
		   + "  `nbFiles` int(11) NOT NULL,"
		   + "  `totalSize` bigint(20) NOT NULL,"
		   + "  KEY `user_ind` (`user`)"
		   + ") ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

		   "DROP TABLE IF EXISTS `L0L_broken`;",
		   "CREATE TABLE `L0L_broken` ("
		   + "  `entryId` bigint(11) NOT NULL,"
		   + "  PRIMARY KEY (`entryId`)"
		   + ") ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

		   "DROP TABLE IF EXISTS `LFN_BOOKED`;",
		   "CREATE TABLE `LFN_BOOKED` ("
		   + "  `lfn` varchar(255) COLLATE latin1_general_cs NOT NULL DEFAULT '',"
		   + "  `owner` varchar(20) CHARACTER SET latin1 COLLATE latin1_general_ci DEFAULT NULL,"
		   + "  `quotaCalculated` smallint(6) DEFAULT NULL,"
		   + "  `existing` smallint(1) DEFAULT NULL,"
		   + "  `jobid` int(11) DEFAULT NULL,"
		   + "  `md5sum` varchar(32) COLLATE latin1_general_cs DEFAULT NULL,"
		   + "  `expiretime` int(11) DEFAULT NULL,"
		   + "  `size` bigint(20) DEFAULT NULL,"
		   + "  `pfn` varchar(255) COLLATE latin1_general_cs NOT NULL DEFAULT '',"
		   + "  `se` varchar(100) CHARACTER SET latin1 COLLATE latin1_general_ci DEFAULT NULL,"
		   + "  `gowner` varchar(20) COLLATE latin1_general_cs DEFAULT NULL,"
		   + "  `user` varchar(20) CHARACTER SET latin1 COLLATE latin1_general_ci DEFAULT NULL,"
		   + "  `guid` binary(16) NOT NULL DEFAULT '\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0',"
		   + "  PRIMARY KEY (`lfn`,`pfn`,`guid`),"
		   + "  KEY `pfn` (`pfn`),"
		   + "  KEY `guid` (`guid`),"
		   + "  KEY `jobid` (`jobid`)"
		   + ") ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

		   "DROP TABLE IF EXISTS `LFN_UPDATES`;",
		   "CREATE TABLE `LFN_UPDATES` ("
		   + "  `guid` binary(16) DEFAULT NULL,"
		   + "  `entryId` int(11) NOT NULL AUTO_INCREMENT,"
		   + "  `action` char(10) COLLATE latin1_general_cs DEFAULT NULL,"
		   + "  PRIMARY KEY (`entryId`),"
		   + "  KEY `guid` (`guid`)"
		   + ") ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

		   "DROP TABLE IF EXISTS `LL_ACTIONS`;",
		   "CREATE TABLE `LL_ACTIONS` ("
		   + "  `tableNumber` int(11) NOT NULL,"
		   + "  `time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,"
		   + "  `action` char(40) COLLATE latin1_general_cs NOT NULL,"
		   + "  `extra` varchar(255) COLLATE latin1_general_cs DEFAULT NULL,"
		   + "  UNIQUE KEY `tableNumber` (`tableNumber`,`action`)"
		   + ") ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

		   "DROP TABLE IF EXISTS `LL_STATS`;",
		   "CREATE TABLE `LL_STATS` ("
		   + "  `tableNumber` int(11) NOT NULL,"
		   + "  `max_time` char(20) COLLATE latin1_general_cs NOT NULL,"
		   + "  `min_time` char(20) COLLATE latin1_general_cs NOT NULL,"
		   + "  UNIQUE KEY `tableNumber` (`tableNumber`)"
		   + ") ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

		   "DROP TABLE IF EXISTS `HOSTS`;",
		   "CREATE TABLE `HOSTS` ("
		   + "  `hostIndex` int(11) NOT NULL AUTO_INCREMENT,"
		   + "  `address` char(50) COLLATE latin1_general_cs,"
		   + "  `db` char(40) COLLATE latin1_general_cs,"
		   + "  `driver` char(10) COLLATE latin1_general_cs,"
		   + "  `organisation` char(40) COLLATE latin1_general_cs,"
	       + "  PRIMARY KEY (`hostIndex`)"
 		   + ") ENGINE=MyISAM AUTO_INCREMENT=2 DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

		   
		   
		};


		
		
		
		
		
		
		
		
		private static String[] catalogueInitialDirectories(){
			
			String[] subfolders = TestConfig.base_home_dir.split("/");
			
			String[] queries = new String[subfolders.length - 1 + 18];
			int b = 0;
			queries[b++] = "USE `"+ TestConfig.dataDB +"`;";
			queries[b++] = "LOCK TABLES `INDEXTABLE` WRITE;";
			queries[b++] = "INSERT INTO `INDEXTABLE` VALUES (0,1,0,'/');";
			queries[b++] = "INSERT INTO `INDEXTABLE` VALUES (0,2,0,'/localdomain/');";
			queries[b++] = "LOCK TABLES `HOSTS` WRITE;";
			queries[b++] = "INSERT INTO `HOSTS` VALUES (1,'" + TestConfig.full_host_name + ":" + TestConfig.sql_port +"','"+TestConfig.dataDB+"','mysql',NULL);";   
			queries[b++] = "INSERT INTO `HOSTS` VALUES (2,'" + TestConfig.full_host_name + ":" + TestConfig.sql_port +"','"+TestConfig.userDB+"','mysql',NULL);";  
			queries[b++] = "LOCK TABLES `L0L` WRITE;";
			int dirIndex = 1;
			int parentdir = dirIndex;
			queries[b++] = "INSERT INTO `L0L` VALUES ("+dirIndex+",'admin',0,'2011-10-06 17:07:26',NULL,NULL,NULL,'',0,NULL,0,"+parentdir+",'admin','d',NULL,NULL,'755');";
		
			
			String path = "/"+ subfolders[0];
			for(int a=1; a< subfolders.length; a++){
				dirIndex++;
				path = path + subfolders[a] + "/";
				System.out.println("path is: -" + path + "-");
				queries[b++] = "INSERT INTO `L0L` VALUES ("+dirIndex+",'admin',0,'2011-10-06 17:07:26',NULL,NULL,NULL,'"+ path +"',0,NULL,0,"+parentdir+",'admin','d',NULL,NULL,'755');";
				parentdir++;
			}
			
			queries[b++] = "UNLOCK TABLES;";
			queries[b++] = "USE `"+ TestConfig.userDB +"`;";
			queries[b++] = "LOCK TABLES `HOSTS` WRITE;";
			queries[b++] = "INSERT INTO `HOSTS` VALUES (1,'" + TestConfig.full_host_name + ":" + TestConfig.sql_port +"','"+TestConfig.dataDB+"','mysql',NULL);";   
			queries[b++] = "INSERT INTO `HOSTS` VALUES (2,'" + TestConfig.full_host_name + ":" + TestConfig.sql_port +"','"+TestConfig.userDB+"','mysql',NULL);";  
			queries[b++] = "LOCK TABLES `INDEXTABLE` WRITE;";
			queries[b++] = "INSERT INTO `INDEXTABLE` VALUES (0,1,0,'/')";
			queries[b++] = "INSERT INTO `INDEXTABLE` VALUES (0,2,0,'/localdomain/');";
			
			queries[b++] = "UNLOCK TABLES;";
		
			return queries;
		}

		
		private static String[] userAddIndexTable(String username, String uid) {
			
			String homeEntryID = queryDB("select entryID from " + TestConfig.dataDB + ".L0L where lfn = '" + TestConfig.base_home_dir + "';","entryID");
				
			return new String[] {
				   "USE `"+ TestConfig.userDB +"`;",
				   "DROP TABLE IF EXISTS `L"+uid+"L`;",
				   "CREATE TABLE `L"+uid+"L` ("
				   + "  `entryId` bigint(11) NOT NULL AUTO_INCREMENT,"
				   + "  `owner` varchar(20) COLLATE latin1_general_cs NOT NULL,"
				   + "  `replicated` smallint(1) NOT NULL DEFAULT '0',"
				   + "  `ctime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,"
				   + "  `guidtime` varchar(8) COLLATE latin1_general_cs DEFAULT NULL,"
				   + "  `jobid` int(11) DEFAULT NULL,"
				   + "  `aclId` mediumint(11) DEFAULT NULL,"
				   + "  `lfn` varchar(255) COLLATE latin1_general_cs DEFAULT NULL,"
				   + "  `broken` smallint(1) NOT NULL DEFAULT '0',"
				   + "  `expiretime` datetime DEFAULT NULL,"
				   + "  `size` bigint(20) NOT NULL DEFAULT '0',"
				   + "  `dir` bigint(11) DEFAULT NULL,"
				   + "  `gowner` varchar(20) COLLATE latin1_general_cs NOT NULL,"
				   + "  `type` char(1) COLLATE latin1_general_cs NOT NULL DEFAULT 'f',"
				   + "  `guid` binary(16) DEFAULT NULL,"
				   + "  `md5` varchar(32) COLLATE latin1_general_cs DEFAULT NULL,"
				   + "  `perm` char(3) COLLATE latin1_general_cs NOT NULL,"
				   + "  PRIMARY KEY (`entryId`),"
				   + "  UNIQUE KEY `lfn` (`lfn`),"
				   + "  KEY `dir` (`dir`),"
				   + "  KEY `guid` (`guid`),"
				   + "  KEY `type` (`type`),"
				   + "  KEY `ctime` (`ctime`),"
				   + "  KEY `guidtime` (`guidtime`)"
				   + ") ENGINE=MyISAM AUTO_INCREMENT=6 DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

				   "DROP TABLE IF EXISTS `L"+uid+"L_QUOTA`;",
				   "CREATE TABLE `L"+uid+"L_QUOTA` ("
				   + "  `user` varchar(64) COLLATE latin1_general_cs NOT NULL,"
				   + "  `nbFiles` int(11) NOT NULL,"
				   + "  `totalSize` bigint(20) NOT NULL,"
				   + "  KEY `user_ind` (`user`)"
				   + ") ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",

				   "DROP TABLE IF EXISTS `L"+uid+"L_broken`;",
				   "CREATE TABLE `L"+uid+"L_broken` ("
				   + "  `entryId` bigint(11) NOT NULL,"
				   + "  PRIMARY KEY (`entryId`)"
				   + ") ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;",
				   
				   "LOCK TABLES `L"+uid+"L` WRITE;",
				   "INSERT INTO `L"+uid+"L` VALUES (5,'"+username +"',0,'2011-10-06 17:07:51',NULL,NULL,NULL,'',0,NULL,0,4,'admin','d',NULL,NULL,'755');",
				
				   "LOCK TABLES `INDEXTABLE` WRITE;",
				   "INSERT INTO `INDEXTABLE` VALUES (0,3,"+uid+",'"+CreateLDAP.getUserHome(username)+"');",


				   "USE `"+ TestConfig.dataDB +"`;",
				   "LOCK TABLES `L0L` WRITE;",
				   "INSERT INTO `L0L` VALUES (0,'admin',0,'2011-10-06 17:07:26',NULL,NULL,NULL,'"+ TestConfig.base_home_dir+username.substring(0,1)+"/" +"',0,NULL,0,"+homeEntryID+",'admin','d',NULL,NULL,'755');",
				   "LOCK TABLES `INDEXTABLE` WRITE;",
				   "INSERT INTO `INDEXTABLE` VALUES (0,3,"+uid+",'"+CreateLDAP.getUserHome(username)+"');",
				   
				   "UNLOCK TABLES;",
				
				};
		}
	   
	
}
