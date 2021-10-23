package com.wavefront.agent.logforwarder.ingestion.processors.model.event.parser;

/**
 * Standard log field name constants
 *
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 9/7/21 1:01 PM
 */
public class FieldConstants {
//  public static final String ID_FIELD = "id";
  public static final String TEXT_FIELD = "text";
  public static final String TIMESTAMP_FIELD = "timestamp";
//  public static final String LOG_TIMESTAMP_FIELD = "log_timestamp";
  public static final String NAME_FIELD = "name";
  public static final String CONTENT_FIELD = "content";
  public static final String STARTING_POS_FIELD = "startPosition";
  public static final String LENGTH_FIELD = "length";
  public static final String SOURCE = "source";

  public FieldConstants() {
  }
}
