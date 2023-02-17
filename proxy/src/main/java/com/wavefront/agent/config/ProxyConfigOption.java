package com.wavefront.agent.config;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

@Retention(RUNTIME)
@Target({FIELD})
public @interface ProxyConfigOption {
  Categories category() default Categories.NA;

  SubCategories subCategory() default SubCategories.NA;

  boolean hide() default false;
}
