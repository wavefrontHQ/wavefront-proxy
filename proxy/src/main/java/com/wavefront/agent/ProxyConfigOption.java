package com.wavefront.agent;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.*;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

@Retention(RUNTIME)
@Target({FIELD})
public @interface ProxyConfigOption {
  String name() default "";

  String category();

  String subCategory() default "";
}
