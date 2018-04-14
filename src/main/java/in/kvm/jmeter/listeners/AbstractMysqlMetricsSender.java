/**
 * 
 */
package in.kvm.jmeter.listeners;

/**
 * @author Madan Kavarthapu
 *
 */
public abstract class AbstractMysqlMetricsSender implements MysqlMetricsSender {

	/**
	 * 
	 */
	public AbstractMysqlMetricsSender() {
		// TODO Auto-generated constructor stub
	}
	
    /**
     * For tag keys, tag values always use a backslash character
     * \ to escape List of special characters : commas , equal sign = spaces
     */
    static final String tagToStringValue(String s) {
        return s.trim().replaceAll(" ", "\\\\ ").replaceAll(",", "\\\\,").replaceAll("=", "\\\\=");
    }
    
    /**
     * For field always use a backslash character
     * \ to escape " characters
     */
    static final String fieldToStringValue(String s) {
        return s.trim().replaceAll("\"", "\\\\\"");
    }	
}
