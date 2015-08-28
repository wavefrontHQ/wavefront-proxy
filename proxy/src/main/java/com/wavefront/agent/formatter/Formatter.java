package com.wavefront.agent.formatter;

/**
 * Abstract formatting class for the push agent to do on-the-fly reformatting of text data, without using a netty pipeline
 * <p/>
 * Created by dev@wavefront.com on 11/5/14.
 */
abstract public class Formatter {

  public Formatter() {

  }

  // Abstract and not static since some future formatters might have state
  abstract public String format(String mesg) throws IllegalArgumentException;
}
