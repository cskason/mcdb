/**
 * 
 */
package com.jk.mcdb.dialet;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import com.jk.mcdb.exception.MCDBException;
import com.jk.mcdb.util.Entity;
import com.jk.mcdb.util.FKey;
import com.jk.mcdb.util.Field;
import com.jk.mcdb.util.Id;

/**
 * @author kason
 *
 */
public class MYSQLDialect extends AbstractDialect{
	
	private Logger log = Logger.getLogger("");

	@Override
	public void init() throws SQLException {
		

	    if ((!this.auto.equalsIgnoreCase("create")) && (!this.auto.equalsIgnoreCase("update"))) 
	    {
	      return ;
	    }
	    
	    List<Class<?>> clazzs = loadClass();
	    
	    this.connect = this.dataSource.getConnection();
	    if (this.auto.equalsIgnoreCase("create")) {
	      this.log.info("开始初使化数据库，初使化类型为：CREATE");
	      create(clazzs);
	    } else if (this.auto.equalsIgnoreCase("update")) {
	      this.log.info("开始初使化数据库，初使化类型为：UPDATE");
	      update(clazzs);
	    }
	    this.sql.addAll(this.alterUpdates);
	    this.sql.addAll(this.foreigns);
	    Statement statement = this.connect.createStatement();
	    for (String obj : this.sql) {
	      if (this.showsql) {
	        System.out.println(obj);
	      }
	      statement.addBatch(obj);
	    }
	    statement.executeBatch();
	    this.connect.close();
	    return ;
		
	}
	
	
	
	private void create(List<Class<?>> clazzs)
	{
		this.sql.add("SET FOREIGN_KEY_CHECKS=0;");
	    for (Class clazz : clazzs) {
	      StringBuffer sqls = new StringBuffer("");
	      String idField = "";
	      if (clazz.isAnnotationPresent(Entity.class))
	      {
	        if (((Entity)clazz.getAnnotation(Entity.class)).tableName().equals("")) {
	          throw new MCDBException("类：[" + clazz.getName() + "]未指定正确的表名!");
	        }
	        this.sql.add("DROP TABLE IF EXISTS " + ((Entity)clazz.getAnnotation(Entity.class)).tableName() + ";");
	        sqls.append("CREATE TABLE " + ((Entity)clazz.getAnnotation(Entity.class)).tableName() + "(\n");
	        java.lang.reflect.Field[] fields = clazz.getDeclaredFields();
	        for (java.lang.reflect.Field field : fields)
	          if (field.isAnnotationPresent(Field.class))
	          {
	            Field fieldAnnotion = (Field)field.getAnnotation(Field.class);
	            if (fieldAnnotion.field().equals("")) {
	              throw new MCDBException("类：" + clazz.getName() + "的属性[" + field.getName() + "]未指定正确的字段名!");
	            }
	            boolean hasIdAnnotion = field.isAnnotationPresent(Id.class);
	            if (hasIdAnnotion) {
	              sqls.append("\t" + fieldAnnotion.field() + " INT(11) NOT NULL AUTO_INCREMENT");
	            } else {
	              FKey fk = fieldAnnotion.fk()[0];
	              if (!fk.tableName().equals("")) {
	                String foreign_key_name = "FK_" + ((Entity)clazz.getAnnotation(Entity.class)).tableName() + "_" + fieldAnnotion.field();
	                StringBuffer foreign = new StringBuffer("");
	                foreign.append("ALTER TABLE " + ((Entity)clazz.getAnnotation(Entity.class)).tableName() + " ");
	                foreign.append("ADD CONSTRAINT " + foreign_key_name + " FOREIGN KEY(" + fieldAnnotion.field() + ") REFERENCES " + fk.tableName() + "(" + fk.fieldName() + ");\n");
	                this.foreigns.add(foreign.toString());
	                sqls.append("\t" + fieldAnnotion.field() + " INT(11)");
	                sqls.append(!fieldAnnotion.nullable() ? " NOT NULL" : "");
	              } else {
	                sqls.append("\t" + fieldAnnotion.field() + " " + fieldAnnotion.type().toString());
	                if (fieldAnnotion.type().toString().endsWith("INT"))
	                  sqls.append("(" + (fieldAnnotion.length() == 255 ? 11 : fieldAnnotion.length()) + ")");
	                else if (fieldAnnotion.type().toString().equals("VARCHAR")) {
	                  sqls.append("(" + fieldAnnotion.length() + ")");
	                }
	                sqls.append(!fieldAnnotion.nullable() ? " NOT NULL" : "");
	              }
	            }
	            if (idField.equals("")) {
	              idField = hasIdAnnotion ? fieldAnnotion.field() : "";
	            }
	            sqls.append(",\n");
	          }
	        if (!idField.equals("")) {
	          sqls.append("\tPRIMARY KEY (" + idField + ")");
	        }
	        sqls.append("\n);\n");
	        this.sql.add(sqls.toString());
	      }
	    }
	}
	
	private void update(List<Class<?>> clazzs)
	{
		for (Class clazz : clazzs) {
		StringBuffer sqls = new StringBuffer("");
	      String idField = "";
	      if (clazz.isAnnotationPresent(Entity.class))
	      {
	        if (((Entity)clazz.getAnnotation(Entity.class)).tableName().equals("")) {
	          throw new MCDBException("类:[" + clazz.getName() + "]未指明正确的表名!");
	        }
	        sqls.append("CREATE TABLE IF NOT EXISTS " + ((Entity)clazz.getAnnotation(Entity.class)).tableName() + "(\n");
	        java.lang.reflect.Field[] fields = clazz.getDeclaredFields();
	        for (java.lang.reflect.Field field : fields)
	          if (field.isAnnotationPresent(Field.class))
	          {
	            Field fieldAnnotion = (Field)field.getAnnotation(Field.class);
	            if (fieldAnnotion.field().equals("")) {
	              throw new MCDBException("类：" + clazz.getName() + "的属性[" + field.getName() + "]未指定正确的字段名!");
	            }
	            boolean hasIdAnnotion = field.isAnnotationPresent(Id.class);
	            if (hasIdAnnotion) {
	              sqls.append("\t" + fieldAnnotion.field() + " INT(11) NOT NULL AUTO_INCREMENT");
	            } else {
	              FKey fk = fieldAnnotion.fk()[0];
	              if (!fk.tableName().equals("")) {
	                String foreign_key_name = "FK_" + ((Entity)clazz.getAnnotation(Entity.class)).tableName() + "_" + fieldAnnotion.field();
	                try {
	                  PreparedStatement ps = this.connect.prepareStatement("SELECT COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE where constraint_name='" + foreign_key_name + "'", 1004, 1007);
	                  ResultSet rs = ps.executeQuery();
	                  rs.last();
	                  if (rs.getRow() == 0) {
	                    StringBuffer foreign = new StringBuffer("");
	                    foreign.append("ALTER TABLE " + ((Entity)clazz.getAnnotation(Entity.class)).tableName() + " ");
	                    foreign.append("ADD CONSTRAINT " + foreign_key_name + " FOREIGN KEY(" + fieldAnnotion.field() + ") REFERENCES " + fk.tableName() + "(" + fk.fieldName() + ");\n");
	                    this.foreigns.add(foreign.toString());
	                  }
	                  sqls.append("\t" + fieldAnnotion.field() + " INT(11)");
	                  sqls.append(!fieldAnnotion.nullable() ? " NOT NULL" : "");
	                } catch (Exception e) {
	                  e.printStackTrace();
	                }
	              } else {
	                sqls.append("\t" + fieldAnnotion.field() + " " + fieldAnnotion.type().toString());
	                if (fieldAnnotion.type().toString().equals("INT"))
	                  sqls.append("(" + (fieldAnnotion.length() == 255 ? 11 : fieldAnnotion.length()) + ")");
	                else if (fieldAnnotion.type().toString().equals("VARCHAR")) {
	                  sqls.append("(" + fieldAnnotion.length() + ")");
	                }
	                sqls.append(!fieldAnnotion.nullable() ? " NOT NULL" : "");
	              }
	            }
	            if (idField.equals(""))
	              idField = hasIdAnnotion ? fieldAnnotion.field() : "";
	            try
	            {
	              String assertField = "DESCRIBE " + ((Entity)clazz.getAnnotation(Entity.class)).tableName() + " " + fieldAnnotion.field();
	              PreparedStatement ps = this.connect.prepareStatement(assertField, 1004, 1007);
	              ResultSet resultSet = ps.executeQuery();
	              resultSet.last();
	              if (resultSet.getRow() == 0) {
	                String type = fieldAnnotion.type().toString();
	                int length = fieldAnnotion.length();
	                String typeSql = "";
	                if (type.equalsIgnoreCase("INT"))
	                  typeSql = "INT(" + (length == 255 ? 11 : length) + ") ";
	                else if (type.equalsIgnoreCase("VARCHAR"))
	                  typeSql = "VARCHAR(" + length + ") ";
	                else {
	                  typeSql = fieldAnnotion.type().toString() + " ";
	                }
	                this.alterUpdates.add("ALTER TABLE " + ((Entity)clazz.getAnnotation(Entity.class)).tableName() + " ADD COLUMN " + fieldAnnotion.field() + " " + typeSql + (fieldAnnotion.nullable() ? "" : "NOT NULL"));
	              }
	            }
	            catch (Exception localException1) {
	            }
	            sqls.append(",\n");
	          }
	        if (!idField.equals("")) {
	          sqls.append("\tPRIMARY KEY (" + idField + ")");
	        }
	        sqls.append("\n);\n");
	        this.sql.add(sqls.toString());
	      }
	    }
	}
	

}
