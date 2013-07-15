package com.clearedgeit.accumulo.flume;

import java.util.List;

import org.apache.accumulo.core.data.Mutation;
import org.apache.flume.Event;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.ConfigurableComponent;

public interface AccumuloEventSerializer extends Configurable,
		ConfigurableComponent {
	/**
	 * Initialize the event serializer.
	 * 
	 * @param Event to be written to Accumulo.
	 */
	public void set(Event event);

	/**
	 * Get the mutations that should be written out to accumulo as a result of
	 * this event.
	 */
	public List<Mutation> getMutations();

	/**
	 * Clean up any state. This will be called when the sink is being stopped.
	 */
	public void close();
}