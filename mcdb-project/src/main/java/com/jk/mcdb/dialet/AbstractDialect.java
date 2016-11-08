/**
 * 
 */
package com.jk.mcdb.dialet;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

/**
 * @author kason
 *
 */
public abstract class AbstractDialect {
	
	

	public List<String> sql = new ArrayList();

	public List<String> foreigns = new ArrayList();

	public List<String> alterUpdates = new ArrayList();
	
	
	  public String[] classNames;
	  public String auto;
	  public boolean showsql;
	  public DataSource dataSource;
	  public Connection connect;
	
	public abstract void init() throws SQLException;
	
	
	
	public List<Class<?>> loadClass()
	{
		
		 List<Class<?>> clazzs = new ArrayList<Class<?>>();
		    for (String className : this.classNames) {
		     
		    try {
		    	clazzs.add(Class.forName(className));
		     
		    }
		    catch (Exception e) {
		      e.printStackTrace();
		     }
		    }
		    return clazzs;
	}
}
