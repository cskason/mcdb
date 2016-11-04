/**
 * 
 */
package com.jk.mcdb.util;


import java.lang.annotation.Annotation;
import java.lang.annotation.Documented;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author kason
 *
 */
@Target({java.lang.annotation.ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
public @interface Field
{
  public abstract String field();

  public abstract FKey[] fk();

  public abstract FieldType type();

  public abstract int length();

  public abstract boolean nullable();
}