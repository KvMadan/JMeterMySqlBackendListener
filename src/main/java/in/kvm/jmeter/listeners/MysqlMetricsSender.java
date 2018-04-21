/**
 * 
 */
package in.kvm.jmeter.listeners;

/**
 * @author Madan Kavarthapu
 *
 */
interface MysqlMetricsSender {

    /**
     * One data point in InfluxDB is represented by a measurement name, a tag
     * set and a field set ( optionally a timestamp )
     */
    final class MetricTuple {
        String measurement;
        String tag;
        String field;
        long timestamp;
        MetricTuple(String measurement, String tag, String field, long timestamp) {
            this.measurement = measurement;
            this.tag = tag;
            this.field = field;
            this.timestamp = timestamp;
        }
    }

    /**
     * 
     * @param measurement 
     * @param tag
     * @param field
     */
    public void addMetric(String measurement, String tag, String field);

	/**
	 * 
	 * @param mysqlHost
	 *            Host
	 * @param mysqlPort
	 *            Port
	 * @param mysqlSchema
	 *            Root Data prefix
	 * @param userName TODO
	 * @param password TODO
	 */
	void setup(String mysqlHost, String mysqlPort, String mysqlSchema, String userName, String password);

	/**
	 * Write metrics to MYSQL.
	 */
	void writeAndSendMetrics();

	/**
	 * Destroy sender
	 */
	void destroy();
}
