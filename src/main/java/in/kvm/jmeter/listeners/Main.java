/**
 * 
 */
package in.kvm.jmeter.listeners;

import java.text.SimpleDateFormat;
import java.sql.Date;
import java.sql.Timestamp;

/**
 * @author Madan Kavarthapu
 *
 */
public class Main {

	/**
	 * 
	 */
	public Main() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		/*GenericMysqlMetricsSender gmms = new GenericMysqlMetricsSender();
		
		gmms.setup("localhost", "3306", "jmetergrafana");
		
		Timestamp dd = new java.sql.Timestamp(Long.parseLong("1522501530773"));
		System.out.println(dd);
		
		//String sb = "events,application=applicaiton_ci,title=ApacheJMeter,text=\"CI-Demo-Test started\",tStamp=1522555509574";
		String sb = "events,application=applicaiton_ci,title=ApacheJMeter,text=\"CI-Demo-Test ended\",tStamp=1522555510306";
		String parts[] = sb.toString().split(",");
		
		System.out.println(">>>>>>>>>>>><<<<<<<<<<<");
        System.out.println(sb.toString());
        System.out.println(">>>>>>>>>>>><<<<<<<<<<<");
  
        for (String s : parts ){
        	if(!s.equalsIgnoreCase("events")){
            	System.out.println(s.split("=")[1]);
        	}
        }
        
        String insertTableSQL = "INSERT INTO CI_Demo_Perf.events"
        		+ "(time, application, text, title) VALUES"
        		+ "(?,?,?,?)";        
        gmms.writeEventsToDB(insertTableSQL, parts);
			*/
		
		Timestamp dd = new java.sql.Timestamp(Long.parseLong("1522501530773"));
		System.out.println(dd);
		
		String s = "jmeter,application=applicaiton_ci,transaction=all,status=all,count=1,countError=0,avg=23.0,min=23.0,max=23.0,"
				+ "hit=3,pct99=23.0,pct95=23.0,pct85=23.0,pct90=23.0,hit=3,tStamp=1522555510307";
		String s1 = "jmeter,application=applicaiton_ci,status=all,transaction=Comcast_Modesto_T01_Authenication_Token,count=1,avg=23.0,min=23.0,max=23.0,pct99.0=23.0,pct95.0=23.0,pct85.0=23.0,pct90.0=23.0,tStamp=1522555510309";
		
		String parts[] = s1.toString().split(",");
		StringBuilder keys = new StringBuilder();
		StringBuilder values = new StringBuilder();
		
		for (int i=1;i<parts.length;i++){
			String part[] = parts[i].split("=");
			if(i != parts.length -1){
				keys.append(part[0] + ",");
				values.append(part[0].equalsIgnoreCase("application") | part[0].equalsIgnoreCase("status") | part[0].equalsIgnoreCase("responsecode") | part[0].equalsIgnoreCase("responsemessage") | part[0].equalsIgnoreCase("transaction") ? "'" + part[1] + "'," : part[1] + ",");
				//values.append(part[1] + ",");
			}
			else {
				keys.append(part[0]);
				values.append("'" + new java.sql.Timestamp(Long.parseLong(part[1])) + "'");
			}
		}

		 StringBuilder query = new StringBuilder();
		 query.append("INSERT INTO CI_DEMO_Perf.jmeter ");
		 query.append("(" + keys.toString() + ") VALUES " );
		 query.append("(" + values.toString() + ")");
		 
		 System.out.println(query);
	}

}
 