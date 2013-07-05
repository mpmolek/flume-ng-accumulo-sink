package com.clearedgeit.accumulo.flume;

/**
* Config constants for the AccumuloSink 
*/

public class AccumuloSinkConfigurationConstants {

	/**
	 * Maximum number of events the sink should take from the channel per
	 * transaction.
	 */
	public static final String CONFIG_BATCHSIZE = "batchSize";

	/**
	 * The fully qualified class name of the serializer the sink should use.
	 */
	public static final String CONFIG_SERIALIZER = "accumulo.serializer";
	
	/**
	 * Comma separated list of zookeeer servers
	 */
	public static final String CONFIG_ZK_SERVERS = "accumulo.zkServers";
	
	/**
	 * Name of the accumulo instance
	 */
	public static final String CONFIG_INTSTANCE = "accumulo.instance";
	
	/**
	 * Accumulo username
	 */
	public static final String CONFIG_USER = "accumulo.user";
	
	/**
	 * Password for the user
	 */
	public static final String CONFIG_PASSWORD = "accumulo.password";

	/**
	 * The Accumulo table which the sink should write to.
	 */
	public static final String CONFIG_TABLE = "accumulo.table";
	
	/**
   * Max memory for the Accumulo BatchWriter
   */
	public static final String CONFIG_MAX_MEMORY = "accumulo.maxMemory";
	
	/**
   * Max latency for the Accumulo BatchWriter
   */
	public static final String CONFIG_MAX_LATENCY = "accumulo.maxLatency";
	
	/**
   * Max write threads for the Accumulo BatchWriter
   */
	public static final String CONFIG_MAX_WRITE_THREADS = "accumulo.maxWriteThreads";
}
