package alien.test;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import alien.config.ConfigUtils;
import alien.monitoring.MonitorFactory;
import apmon.ApMon;
import apmon.ApMonException;

public class CassandraMonitor {

	public static final String serviceUrl = "service:jmx:rmi://127.0.0.1/jndi/rmi://127.0.0.1:7199/jmxrmi";
	public static JMXServiceURL url;
	public static JMXConnector jmxc = null;
	public static MBeanServerConnection mbsConnection = null;
	static transient final Logger logger = ConfigUtils.getLogger(CassandraMonitor.class.getCanonicalName());
	static transient final ApMon apmon = MonitorFactory.getApMonSender();
	static String hostName = null;
	
	static final String[] metrics = {
			"org.apache.cassandra.metrics:type=ClientRequest,scope=Write,name=TotalLatency",
			"org.apache.cassandra.metrics:type=ClientRequest,scope=Read,name=TotalLatency",
			"org.apache.cassandra.metrics:type=ClientRequest,scope=Write,name=Latency",
			"org.apache.cassandra.metrics:type=ClientRequest,scope=Read,name=Latency",
			"org.apache.cassandra.metrics:type=ClientRequest,scope=Write,name=Latency",
			"org.apache.cassandra.metrics:type=ClientRequest,scope=Read,name=Latency",
			"org.apache.cassandra.metrics:type=Cache,scope=KeyCache,name=Hits",
			"org.apache.cassandra.metrics:type=Cache,scope=KeyCache,name=Requests",
			"org.apache.cassandra.metrics:type=Storage,name=Load",
			"org.apache.cassandra.metrics:type=Compaction,name=CompletedTasks",
			"java.lang:type=GarbageCollector,name=ConcurrentMarkSweep",
			"java.lang:type=GarbageCollector,name=ConcurrentMarkSweep",
			"org.apache.cassandra.metrics:type=Storage,name=Exceptions",
			"org.apache.cassandra.metrics:type=ClientRequest,scope=Read,name=Timeouts",
			"org.apache.cassandra.metrics:type=ClientRequest,scope=Write,name=Timeouts",
			"org.apache.cassandra.metrics:type=ClientRequest,scope=Read,name=Unavailables",
			"org.apache.cassandra.metrics:type=ClientRequest,scope=Write,name=Unavailables"
			};
	static final String[] attributes = {
			"Count",
			"Count",
			"OneMinuteRate",
			"OneMinuteRate",
			"Count",
			"Count",
			"OneMinuteRate",
			"OneMinuteRate",
			"Count",
			"Value",
			"CollectionCount",
			"CollectionTime",
			"Count",
			"Count",
			"Count",
			"Count",
			"Count"
			};

	static final String[] names = {
			"Write_TotalLatency_Count",
			"Read_TotalLatency_Count",
			"Write_Latency_OneMinuteRate",
			"Read_Latency_OneMinuteRate",
			"Write_Latency_Count",
			"Read_Latency_Count",
			"KeyCache_Hits_OneMinuteRate",
			"KeyCache_Requests_OneMinuteRate",
			"Storage_Load_Count",
			"Compaction_CompletedTasks_Value",
			"ConcurrentMarkSweep_CollectionCount",
			"ConcurrentMarkSweep_CollectionTime",
			"Storage_Exceptions_Count",
			"Read_Timeouts_Count",
			"Write_Timeouts_Count",
			"Read_Unavailables_Count",
			"Write_Unavailables_Count"
			};	

	public static void main(String[] args) {
		try {
			hostName = InetAddress.getLocalHost().getCanonicalHostName();
			url = new JMXServiceURL(serviceUrl);
		} catch (Exception e1) {
			System.err.println("Exception creating JMXServiceURL or JMXConnector: " + e1);
			logger.log(Level.SEVERE, "Exception creating JMXServiceURL: " + e1);
			System.exit(-1);
		}
		
		if(metrics.length != attributes.length || metrics.length != names.length){
			System.err.println("Metrics-attributes-names don't match");
			logger.log(Level.SEVERE, "Metrics-attributes-names don't match");
			System.exit(-1);		
		}
		
		while (true) {
			try {

				Vector<String> paramNames = new Vector<>();
				Vector<Object> paramValues = new Vector<>();

				for (int i = 0; i < metrics.length; i++) {
					Object obj = (Object) getMbeanAttributeValue(metrics[i], attributes[i]);
					//System.out.println("Object: " + obj.toString());
					paramNames.add(names[i]);
					if(obj instanceof Long) 
				        paramValues.add(((Long) obj).doubleValue());
					else
					    paramValues.add((Double) obj);
				}

				try {
					logger.log(Level.INFO, "Sending: "+paramNames.toString()+" and "+paramValues.toString());
					apmon.sendParameters("Cassandra_Nodes", hostName, paramNames.size(), paramNames, paramValues);
				} catch (ApMonException e) {
					System.err.println("Exception sending parameters: " + e);
					logger.log(Level.SEVERE, "Exception sending parameters: " + e);
				}

			} catch (AttributeNotFoundException | InstanceNotFoundException | MalformedObjectNameException | MBeanException | ReflectionException | IOException e) {
				System.err.println("Exception getting attribute: " + e);
				logger.log(Level.SEVERE, "Exception getting attribute: " + e);
			}

			//System.out.println("Sleep for 60 seconds...");
			logger.log(Level.INFO, "Sleep for 60 seconds...");

			try {
				Thread.sleep(60000);
			} catch (InterruptedException e2) {
				System.err.println("Exception sleeping: " + e2);
				logger.log(Level.SEVERE, "Exception sleeping: " + e2);
			}
		}
	}

	private static Object getMbeanAttributeValue(String MbeanObjectName, String attributeName)
			throws IOException, AttributeNotFoundException, InstanceNotFoundException, MBeanException, ReflectionException, MalformedObjectNameException {
		Object attributeValue = null;
		try {
			jmxc = JMXConnectorFactory.connect(url, null);
			mbsConnection = jmxc.getMBeanServerConnection();
			
			ObjectName objectName = new ObjectName(MbeanObjectName);
			attributeValue = mbsConnection.getAttribute(objectName, attributeName);

			// try {
			// attributeValue = mbsConnection.getMBeanInfo(objectName);
			// } catch (IntrospectionException e) {
			// e.printStackTrace();
			// }

		} finally {
			if (jmxc != null)
				jmxc.close();
		}
		return attributeValue;
	}
}