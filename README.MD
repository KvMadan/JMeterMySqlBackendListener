# Overview 

### Description 
JMeter MySQL Backend Listener is a JMeter plugin enabling you to send the test results to a MySQL database. It is meant as an alternative live monitoring tool to the build-in "InfluxDB" backend listener of JMeter. 

### Prerequisites
- JDK 1.8
- JMeter 3.3+
- MySql Connector - 5.1.6

### Download & Install 
1. Download Jar file from the release page
2. Copy the file to $JMETER_HOME/lib/ext folder 
3. Download mysql-connector-java and copy it to $JMETER_HOME/lib/ext
4. Restart JMeter if already running

### Usage
1. Add Backend Listener to the JMeter Test Plan 
2. Select Backend Listener Implementation as 
3. configure the options

### Configuration 

#### Arguments 
Set the below Backend Listener properties from the JMeter UI

| Argument | Description | Default Value |
| -------- | ----------- | ------------- |
| **MYSQLMetricsSender** | Class that will send statistics | Standard MySQL sender class |
| **MYSQLServer** | MySql Server to which the metrics to be sent | localhost
| **MYSQLPort** | MySql Server port | 3306 | 
| **MYSQLDatabaseName** | MySql Database name | jmeter | 
| **application** | Application Name | application name | 
| **measurement** | Defautl table for the measurements | jmeter |
| **summaryOnly** | Send summary details only to the DB | false |
| **samplersRegex** | regular expression for samplers filtering | .* |
| **percentiles** | Percentile values to be sent to DB | 99;95;90 | 
| **testTitle** | Name of the Test to be stored in DB | "Test name" |
| **eventTags** | Name of the event Tags | "" | 

