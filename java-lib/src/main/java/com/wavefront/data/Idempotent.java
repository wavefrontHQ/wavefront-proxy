package com.wavefront.data;

import java.io.IOException;
import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.*;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Idempotent calls can be retried if a call fails.
 *
 * @author Clement Pang (clement@sunnylabs.com)
 */
@Target(value = {METHOD, PARAMETER, TYPE})
@Retention(RUNTIME)
@Documented
public @interface Idempotent {
  /**
   * @return Number of times to retry a call when it fails.
   */
  int retries() default 3;

  /**
   * @return List of exceptions that should be retried.
   */
  Class[] retryableExceptions() default IOException.class;
}
