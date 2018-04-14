/**
 * 
 */
package in.kvm.jmeter.listeners;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Madan Kavarthapu
 *
 */
class GenericMysqlMetricsSender extends AbstractMysqlMetricsSender {

	private static final Logger log = LoggerFactory.getLogger(GenericMysqlMetricsSender.class);
	
	private final Object lock = new Object();
	
	private List<MetricTuple> metrics = new ArrayList();
	
	private Future<Integer> lastRequest;
	
	private String JDBC_DRIVER = "com.mysql.jdbc.Driver";
	private String DB_URL = "jdbc:mysql://%s:%s/";
	private Connection connection = null;
	private Statement statement;
	private PreparedStatement eventsPreparedStatement;

	private String DB_CREATE_SQL = "CREATE DATABASE %s";
	private String TABLE_CREATE_EVENTS_SQL = "CREATE TABLE IF NOT EXISTS  %s.events ("
			+ "  `time` timestamp NOT NULL,"
			+ "  `application` varchar(30),"
			+ "  `text` varchar(40),"
			+ "  `title` varchar(20))";
	private String TABLE_CREATE_JMETER_SQL = "CREATE TABLE IF NOT EXISTS %s.jmeter ("
			+ "  `tStamp` timestamp NOT NULL,"
			+ "  `application` varchar(30),"
			+ "  `avg` decimal(7,3),"
			+ "  `count` int(11),"
			+ "  `countError` int(11),"
			+ "  `endedT` int(11),"
			+ "  `hit` int(11),"
			+ "  `max` decimal(7,3),"
			+ "  `maxAT` decimal(7,3),"
			+ "  `meanAT` decimal(7,3),"
			+ "  `min` decimal(7,3),"
			+ "  `minAT` decimal(7,3),"
			+ "  `pct90` decimal(7,3),"
			+ "  `pct95` decimal(7,3),"
			+ "  `pct99` decimal(7,3),"
			+ "  `startedT` int(11),"
			+ "  `status`varchar(5),"
			+ "  `transaction` varchar(256),"
			+ "  `responsecode` varchar(256),"
			+ "  `responsemessage` varchar(512))";

	private String INSERT_EVENTS_SQL = "INSERT INTO %s.events"
    		+ "(time, application, text, title) VALUES"
    		+ "(?,?,?,?)";
	
	private String INSERT_TRANSACTIONS_SQL = "INSERT INTO %s.jmeter ( %s ) VALUES ( %s )";
	
/*	
 * private String INSERT_TRANSACTIONS_SQL = "INSERT INTO CI_Demo_Perf.jmeter"
			+ "(time, application, avg, count, countError, endedT, hit, max, maxAT, meanAT, min, minAT,"
			+ "pct90, pct95, pct99, startedT, status, transaction, responsecode, responsemessage) VALUES"
			+ "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
*/
	
	/**
	 * Constructor
	 */
	GenericMysqlMetricsSender() {
		super();
	}

	/* (non-Javadoc)
	 * @see in.kvm.jmeter.listeners.MysqlMetricsSender#addMetric(long, java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
    public void addMetric(String mesurement, String tag, String field) {
        synchronized (lock) {
            metrics.add(new MetricTuple(mesurement, tag, field, System.currentTimeMillis()));            
        }
    }

	/* (non-Javadoc)
	 * @see in.kvm.jmeter.listeners.MysqlMetricsSender#setup(java.lang.String, int, java.lang.String)
	 */
	@Override
	public void setup(String mysqlHost, String mysqlPort, String mysqlSchema) {
		boolean schemaExists = false;

		try {
			Class.forName(JDBC_DRIVER);
			connection = DriverManager.getConnection(String.format(DB_URL, mysqlHost, mysqlPort),"root", "root");
			
			ResultSet resultSet = connection.getMetaData().getCatalogs();
			
			//iterate each schema in the ResultSet
			log.debug("**List of available Schemas**");
			while (resultSet.next()) {
			  // Get the database name, which is at position 1
			  String databaseName = resultSet.getString(1);
			  log.debug(databaseName);
			  if (databaseName.equalsIgnoreCase(mysqlSchema)){
				  log.debug("Database already exists");
				  schemaExists = true;
				  break;
			  }
			}
			resultSet.close();
			
			if (!schemaExists){
				//Create a new Schema with the given name. 
				statement = connection.createStatement();
			    statement.executeUpdate(String.format(DB_CREATE_SQL, mysqlSchema));
			    log.debug("Database created successfully..");
				//Create Required Tables in DB. 
				statement.executeUpdate(String.format(TABLE_CREATE_EVENTS_SQL, mysqlSchema));
				statement.executeUpdate(String.format(TABLE_CREATE_JMETER_SQL, mysqlSchema));
			}
			
			//Update Schema name
			INSERT_EVENTS_SQL = String.format(INSERT_EVENTS_SQL, mysqlSchema);
			INSERT_TRANSACTIONS_SQL = String.format(INSERT_TRANSACTIONS_SQL, mysqlSchema, "%s", "%s");
			
			connection.setCatalog(mysqlSchema);
			log.debug("Catalog: " + connection.getCatalog());

			//move this to destroy method.
			//connection.close();
			
		} catch (ClassNotFoundException e) {
			log.error("Class Not found: " + JDBC_DRIVER);
			e.printStackTrace();
		} catch (SQLException e) {
			log.error(e.getMessage());
			e.printStackTrace();
		}
	}

	/* (non-Javadoc)
	 * @see in.kvm.jmeter.listeners.MysqlMetricsSender#writeAndSendMetrics()
	 */
	@Override
	public void writeAndSendMetrics() {
       List<MetricTuple> tempMetrics;
        synchronized (lock) {
            if(metrics.isEmpty()) {
                return;
            }
            tempMetrics = metrics;
            metrics = new ArrayList<>(tempMetrics.size());            
        }
        final List<MetricTuple> copyMetrics = tempMetrics;
        if (!copyMetrics.isEmpty()) {
                if(statement == null) {
                    //httpRequest = createRequest(url);
                }
                StringBuilder sb = new StringBuilder(copyMetrics.size()*35);
                String parts[];
                for (MetricTuple metric : copyMetrics) {
                    // Add TimeStamp in nanosecond from epoch ( default in InfluxDB )
                    sb.append(metric.measurement)
                        .append(metric.tag)
                        .append(",") //$NON-NLS-1$
                        .append(metric.field)
                        .append(",")
                        .append("tStamp="+metric.timestamp) 
                        .append("\n"); //$NON-NLS-1$
                    log.debug(" >>> " +  metric.tag + " : " + metric.field + " : " + metric.timestamp);
                }
                
                log.debug(">>>>>>>>>>>><<<<<<<<<<<");
                log.debug(sb.toString());
                log.debug(">>>>>>>>>>>><<<<<<<<<<<");
                parts = sb.toString().split("\n");
                for (String s : parts){
                	//log.debug(s);
                	String internalParts[] = s.split(",");
                	for (String ss: internalParts){
                		
                		
		                if (ss.equals("events"))
		                {
		                	//log.debug(">>Writing Events to Database<<");
		                	writeEventsToDB(INSERT_EVENTS_SQL, internalParts);
		                }
		                else if (ss.equals("jmeter")){
		                	//log.debug(">>Writing JMeter Transactions to Database<<");
		                	writeTransactionsToDB(internalParts);
		                }
		                
		                //for (String sss : internalParts ){
		                	//log.debug(sss);
		                //}
                	}
                }	
        }

        // We drop metrics in all cases
        copyMetrics.clear(); 		
	}

	/* (non-Javadoc)
	 * @see in.kvm.jmeter.listeners.MysqlMetricsSender#destroy()
	 */
	@Override
	public void destroy() {
		//closing the MYSQL Connection 
		try {
			connection.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		finally{
			try {
				connection.close();
			} catch (SQLException e) {
			}
		}
	}
	
	public void writeTransactionsToDB(String[] parts){
		/*  "INSERT INTO CI_Demo_Perf.jmeter"
			+ "(time, application, avg, count, countError, endedT, hit, max, maxAT, meanAT, min, minAT,"
			+ "pct90, pct95, pct99, startedT, status, transaction, responsecode, responsemessage) VALUES"
			+ "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		String sb = jmeter,application=applicaiton_ci,transaction=all,status=all,count=1,countError=0,avg=23.0,min=23.0,
		max=23.0,hit=3,pct99.0=23.0,pct95.0=23.0,pct85.0=23.0,pct90.0=23.0,hit=3,tStamp=1522555510307000000
		*/
		
		log.debug("Writing Transactions to DB");
		 
		StringBuilder keys = new StringBuilder();
		StringBuilder values = new StringBuilder();
		
		for (int i=1;i<parts.length;i++){
			String part[] = parts[i].split("=");
			if(i != parts.length -1){
				keys.append(part[0] + ",");
				//values.append(part[1] + ",");
				values.append(part[0].equalsIgnoreCase("application") | part[0].equalsIgnoreCase("status") | part[0].equalsIgnoreCase("responsecode") | part[0].equalsIgnoreCase("responsemessage") | part[0].equalsIgnoreCase("transaction") ? "'" + part[1] + "'," : part[1] + ",");
			}
			else {
				keys.append(part[0]);
				//values.append(part[1]);
				values.append("'" + new java.sql.Timestamp(Long.parseLong(part[1])) + "'");
			}
		}
		try {
			
			StringBuilder query = new StringBuilder();
			//query.append("INSERT INTO CI_DEMO_Perf.jmeter " + "(" + keys.toString() + ") VALUES " + 
			//			"(" + values.toString() + ")");
			query.append(String.format(INSERT_TRANSACTIONS_SQL, keys.toString(), values.toString()));
			
			log.debug("QUERY: " + query.toString());
			statement = connection.createStatement();
			int row = statement.executeUpdate(query.toString());
			log.debug("Inserted: " + row);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		catch(Exception e){
			e.printStackTrace();
		}

	}
	
	public void writeEventsToDB(String query, String[] parts){
		
        /*String insertTableSQL = "INSERT INTO CI_Demo_Perf.events"
        		+ "(time, application, text, title) VALUES"
        		+ "(?,?,?,?)"; 
        String sb = 
        "events,application=applicaiton_ci,title=ApacheJMeter,text=\"CI-Demo-Test started\",tStamp=1522555509574";
        */

		try {
			log.debug("Writing Events to DB");
			//log.debug(parts[1].split("=")[1] + " : " + parts[2].split("=")[1] + " : " + parts[3].split("=")[1] + " : " + new java.sql.Timestamp(Long.parseLong(parts[4].split("=")[1])));
			eventsPreparedStatement = connection.prepareStatement(query);
			eventsPreparedStatement.setTimestamp(1, new java.sql.Timestamp(Long.parseLong(parts[4].split("=")[1])));
			eventsPreparedStatement.setString(2, parts[1].split("=")[1]);
			eventsPreparedStatement.setString(3, parts[3].split("=")[1]);
			eventsPreparedStatement.setString(4, parts[2].split("=")[1]);
			
			eventsPreparedStatement.execute();
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
