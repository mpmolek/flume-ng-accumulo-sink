package com.clearedgeit.accumulo.flume;

import java.util.LinkedList;
import java.util.List;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.data.Mutation;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * An Accumulo sink for flume-ng. For each event it processes, it gets a list of mutations from a configurable AccumuloEventSerializer.
 */

public class AccumuloSink extends AbstractSink implements Configurable {
  
  private long eventsProcessed;
  
  private String instance;
  private String zkServers;
  private String user;
  private String password;
  
  private String tableName;
  private long maxMemory;
  private long maxLatency;
  private int maxWriteThreads;
  
  private String serializerClass;
  private AccumuloEventSerializer serializer;
  
  private Connector conn = null;
  private BatchWriter writer;
  private long batchSize;
  
  private SinkCounter sinkCounter;
  
  static private Logger logger = LoggerFactory.getLogger(AccumuloSink.class);
  
  public AccumuloSink() {
    super();
  };
  
  public AccumuloSink(Connector accumuloConnector) throws AccumuloException, AccumuloSecurityException {
    this();
    this.conn = accumuloConnector;
  }
  
  @Override
  public void configure(Context context) {
    
    // these options are required to set up the Connector,
    // but if it already exists, they aren't necessary.
    if (this.conn == null) {
      this.instance = Preconditions.checkNotNull(context.getString(AccumuloSinkConfigurationConstants.CONFIG_INTSTANCE),
          AccumuloSinkConfigurationConstants.CONFIG_INTSTANCE + " is required");
      
      this.zkServers = Preconditions.checkNotNull(context.getString(AccumuloSinkConfigurationConstants.CONFIG_ZK_SERVERS),
          AccumuloSinkConfigurationConstants.CONFIG_ZK_SERVERS + " is required");
      
      this.user = Preconditions.checkNotNull(context.getString(AccumuloSinkConfigurationConstants.CONFIG_USER), AccumuloSinkConfigurationConstants.CONFIG_USER
          + " is required");
      
      this.password = Preconditions.checkNotNull(context.getString(AccumuloSinkConfigurationConstants.CONFIG_PASSWORD),
          AccumuloSinkConfigurationConstants.CONFIG_PASSWORD + " is required");
    }
    
    this.tableName = Preconditions.checkNotNull(context.getString(AccumuloSinkConfigurationConstants.CONFIG_TABLE),
        AccumuloSinkConfigurationConstants.CONFIG_TABLE + " is required");
    
    this.serializerClass = context.getString(AccumuloSinkConfigurationConstants.CONFIG_SERIALIZER,
        "com.clearedgeit.accumulo.flume.SimpleAccumuloEventSerializer");
    
    this.batchSize = context.getLong(AccumuloSinkConfigurationConstants.CONFIG_BATCHSIZE, new Long(100));
    
    this.maxMemory = context.getLong(AccumuloSinkConfigurationConstants.CONFIG_MAX_MEMORY, 1000000L);
    
    this.maxLatency = context.getLong(AccumuloSinkConfigurationConstants.CONFIG_MAX_LATENCY, 1000L);
    
    this.maxWriteThreads = context.getInteger(AccumuloSinkConfigurationConstants.CONFIG_MAX_WRITE_THREADS, 2);
    
    // Initialize the event serializer
    logger.info("Using serializer: " + this.serializerClass);
    
    Class<? extends AccumuloEventSerializer> clazz;
    try {
      clazz = Class.forName(this.serializerClass).asSubclass(AccumuloEventSerializer.class);
      this.serializer = clazz.newInstance();
      this.serializer.configure(context);
    } catch (ClassNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (InstantiationException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    
    this.sinkCounter = new SinkCounter(this.getName());
  }
  
  @Override
  public void start() {
    
    this.eventsProcessed = 0;
    
    // Initialize the connection to Accumulo that
    // this Sink will forward Events to ..
    
    try {
      if (this.conn == null) {
        ZooKeeperInstance inst = new ZooKeeperInstance(this.instance, this.zkServers);
        this.conn = inst.getConnector(this.user, this.password.getBytes());
      }
      this.writer = this.conn.createBatchWriter(this.tableName, this.maxMemory, this.maxLatency, this.maxWriteThreads);
    } catch (TableNotFoundException e) {
      System.err.println("Table \"" + this.tableName + "\" not found");
      e.printStackTrace();
    } catch (AccumuloException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (AccumuloSecurityException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    
    super.start();
  }
  
  @Override
  public Status process() throws EventDeliveryException {
    
    Status status = Status.READY;
    
    List<Mutation> mutations = new LinkedList<Mutation>();
    
    // Start transaction
    Channel ch = getChannel();
    Transaction txn = ch.getTransaction();
    txn.begin();
    try {
      long i = 0;
      for (; i < batchSize; i++) {
        Event event = ch.take();
        if (event == null) {
          status = Status.BACKOFF;
          if (i == 0) {
            sinkCounter.incrementBatchEmptyCount();
          } else {
            sinkCounter.incrementBatchUnderflowCount();
          }
          break;
        } else {
          this.serializer.set(event);
          mutations.addAll(serializer.getMutations());
          eventsProcessed++;
        }
      }
      if (i == batchSize) {
        sinkCounter.incrementBatchCompleteCount();
      }
      sinkCounter.addToEventDrainAttemptCount(i);
      
      writer.addMutations(mutations);
      txn.commit();
      
    } catch (Throwable t) {
      
      txn.rollback();
      
      // Log exception, handle individual exceptions as needed
      t.printStackTrace();
      
      status = Status.BACKOFF;
      
      // re-throw all Errors
      if (t instanceof Error) {
        throw (Error) t;
      }
    } finally {
      txn.close();
    }
    return status;
  }
  
  @Override
  public void stop() {
    if (this.writer != null) {
      try {
        this.writer.close();
        this.writer = null;
      } catch (MutationsRejectedException e) {
        e.printStackTrace();
        this.writer = null;
      }
    }
  }
}
