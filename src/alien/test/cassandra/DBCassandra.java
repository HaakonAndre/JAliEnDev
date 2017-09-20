package alien.test.cassandra;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.logging.Logger;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;

import alien.config.ConfigUtils;
import lazyj.ExtProperties;

/**
 * @author mmmartin
 *
 */
public class DBCassandra {
	private static DBCassandra dbc = new DBCassandra();
	private Session session = null;
	private Cluster cluster = null;
	private static transient final Logger logger = ConfigUtils.getLogger(DBCassandra.class.getCanonicalName());

	private DBCassandra() {
		// Create the connection pool
		final PoolingOptions poolingOptions = new PoolingOptions();
		poolingOptions.setConnectionsPerHost(HostDistance.LOCAL, 2, 2).setConnectionsPerHost(HostDistance.REMOTE, 1, 1);

		// SocketOptions socketOptions = new SocketOptions();
		// socketOptions.setReadTimeoutMillis(12000);
		ExtProperties config = ConfigUtils.getConfiguration("cassandra");
		if (config == null) {
			logger.severe("cassandra.properties missing?");
			return;
		}

		String nodes = config.gets("cassandraNodes");
		String user = config.gets("cassandraUsername");
		String pass = config.gets("cassandraPassword");

		if (nodes.equals("") || user.equals("") || pass.equals("")) {
			logger.severe("cassandra.properties misses some field: cassandraNodes or cassandraUsername or cassandraPassword");
			return;
		}

		String[] ns = nodes.split(",");
		ArrayList<InetAddress> addresses = new ArrayList<>();
		for (String node : ns) {
			try {
				addresses.add(InetAddress.getByName(node));
			} catch (UnknownHostException e) {
				logger.severe("Cannot create InetAddress from: " + node + " - Exception: " + e);
			}
		}

		cluster = Cluster.builder().addContactPoints(addresses).withLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy())).withPoolingOptions(poolingOptions)
				// .withSocketOptions(socketOptions)
				.withCredentials(user, pass).build();

		session = cluster.connect();
	}

	/**
	 * Static 'instance' method
	 *
	 * @return the instance
	 */
	public static Session getInstance() {
		return dbc.session;
	}

	/**
	 *
	 */
	public static void shutdown() {
		dbc.session.close();
		dbc.cluster.close();
	}
}
