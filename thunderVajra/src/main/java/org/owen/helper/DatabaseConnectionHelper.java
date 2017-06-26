package org.owen.helper;

import java.sql.SQLException;
import java.util.Timer;

import org.apache.log4j.Logger;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.apache.tomcat.jdbc.pool.PoolProperties;
import org.rosuda.REngine.REXP;
import org.rosuda.REngine.REXPMismatchException;
import org.rosuda.REngine.Rserve.RConnection;
import org.rosuda.REngine.Rserve.RserveException;

public class DatabaseConnectionHelper {

	static DatabaseConnectionHelper dch;

	static public DatabaseConnectionHelper getDBHelper() {
		if (dch == null) {
			dch = new DatabaseConnectionHelper();
		}
		return dch;
	}

	public DataSource masterDS;
	private RConnection rCon;

	private boolean rConInUse = false;
	Timer timer = new Timer();

	private final static String MASTER_URL = UtilHelper.getConfigProperty("master_sql_url");
	private final static String MASTER_USER = UtilHelper.getConfigProperty("master_sql_user");
	private final static String MASTER_PASSWORD = UtilHelper.getConfigProperty("master_sql_password");

	public DatabaseConnectionHelper() {

		PoolProperties p = new PoolProperties();
		p.setUrl(MASTER_URL);
		p.setDriverClassName("com.mysql.jdbc.Driver");
		p.setUsername(MASTER_USER);
		p.setPassword(MASTER_PASSWORD);
		p.setJmxEnabled(true);
		p.setTestWhileIdle(true); 
		p.setTestOnBorrow(true);
		p.setValidationQuery("SELECT 1");
		p.setTestOnReturn(false);
		p.setValidationInterval(30000);
		p.setTimeBetweenEvictionRunsMillis(30000);
		p.setMaxActive(100);
		p.setInitialSize(10);
		p.setMaxWait(10000);
		p.setRemoveAbandonedTimeout(60);
		p.setMinEvictableIdleTimeMillis(30000);
		p.setMinIdle(10);
		p.setLogAbandoned(true);
		p.setRemoveAbandoned(true);
		p.setConnectionProperties("connectionTimeout=\"300000\"");
		p.setJdbcInterceptors("org.apache.tomcat.jdbc.pool.interceptor.ConnectionState;"
				+ "org.apache.tomcat.jdbc.pool.interceptor.ResetAbandonedTimer");
		masterDS = new DataSource();
		masterDS.setPoolProperties(p);

		// R connection
		try {
			rCon = (rCon != null && rCon.isConnected()) ? rCon : new RConnection();
			Logger.getLogger(DatabaseConnectionHelper.class).debug("Successfully connected to R");
			String rScriptPath = UtilHelper.getConfigProperty("r_script_path");
			String workingDir = "setwd(\"" + rScriptPath + "\")";
			Logger.getLogger(DatabaseConnectionHelper.class).debug("Trying to load the RScript file at " + rScriptPath);
			rCon.eval(workingDir);
			String s = "source(\"scriptLnT.r\")";
			Logger.getLogger(DatabaseConnectionHelper.class).debug("R Path for eval " + s + ".... Loading now ...");

			REXP loadRScript = rCon.eval(s);
			if (loadRScript.inherits("try-error")) {
				Logger.getLogger(DatabaseConnectionHelper.class).error(
						"An error occurred while trying to loading the R script : " + loadRScript.asString());
				releaseRcon();
				throw new REXPMismatchException(loadRScript, "Error: " + loadRScript.asString());
			} else {
				Logger.getLogger(DatabaseConnectionHelper.class).debug("Successfully loaded scriptLnT.r script");
			}
			Logger.getLogger(DatabaseConnectionHelper.class).info("HashMap created!!!");

		} catch (RserveException | REXPMismatchException e) {
			Logger.getLogger(DatabaseConnectionHelper.class).error("An error occurred while trying to connect to R", e);
		}

	}

	@Override
	public void finalize() {
		Logger.getLogger(DatabaseConnectionHelper.class).debug("Shutting down databases ...");
		try {
			if (!masterDS.getConnection().isClosed()) {
				try {
					masterDS.getConnection().close();
					masterDS.close();
					Logger.getLogger(DatabaseConnectionHelper.class).debug("Connection to master database closed!!!!");
				} catch (SQLException e) {
					Logger.getLogger(DatabaseConnectionHelper.class).error("An error occurred while closing the mysql connection", e);
				}
			}

			if (rCon.isConnected()) {
				rCon.close();
				Logger.getLogger(DatabaseConnectionHelper.class).debug("Connection to R closed!!!!");
			}

		} catch (SQLException e) {
			Logger.getLogger(DatabaseConnectionHelper.class).error("An error occurred while attempting to close db connections", e);
		}
	}

	public RConnection getRConn() {
		Logger.getLogger(DatabaseConnectionHelper.class).debug("Entering the get R connection function");
		while (rConInUse)
			try {
				Thread.sleep(100);
				Logger.getLogger(DatabaseConnectionHelper.class).debug("Waiting for R connection");
			} catch (InterruptedException e) {
				Logger.getLogger(DatabaseConnectionHelper.class).error("An error occurred while trying to get the R connection", e);
			}
		Logger.getLogger(DatabaseConnectionHelper.class).debug("RConnection provided...");
		rConInUse = true;
		return rCon;

	}

	public void releaseRcon() {
		Logger.getLogger(DatabaseConnectionHelper.class).debug("Releasing R connection");
		rConInUse = false;
	}
}
