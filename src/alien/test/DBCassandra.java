package alien.test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;

public class DBCassandra {
	private static DBCassandra dbc = new DBCassandra();
	private Session session = null;
	private Cluster cluster = null;

	private DBCassandra() {
		// Create the connection pool
		PoolingOptions poolingOptions = new PoolingOptions();
		poolingOptions.setConnectionsPerHost(HostDistance.LOCAL, 1, 1).setConnectionsPerHost(HostDistance.REMOTE, 1, 1);
		
		SocketOptions socketOptions = new SocketOptions();
		socketOptions.setReadTimeoutMillis(12000);

        cluster = Cluster
                .builder()
                .addContactPoints("alientest02.cern.ch", "aliendb06f.cern.ch",
                                "alientest05.cern.ch", "alientest06.cern.ch", "alientest07.cern.ch")
                .withLoadBalancingPolicy(
                                new TokenAwarePolicy(new RoundRobinPolicy()))
                .withPoolingOptions(poolingOptions)
                .withSocketOptions(socketOptions)
                .withCredentials("cassandra", "cassandra").build();
		
		session = cluster.connect();
	}

	/* Static 'instance' method */
	public static Session getInstance() {
		return dbc.session;
	}

	public static void shutdown() {
		dbc.session.close();
		dbc.cluster.close();
	}
}
