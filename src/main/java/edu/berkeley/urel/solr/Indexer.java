package edu.berkeley.urel.solr;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.ResourceBundle;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Indexer extends Thread {
	private static Logger logger = LoggerFactory.getLogger(Indexer.class);

	private SolrClient server;
	private String query;
	private Runtime runtime = Runtime.getRuntime();
	private DecimalFormat df = new DecimalFormat("#.00");


	public Indexer(SolrClient server, String query) {
		this.server = server;
		this.query = query;

	}

	private void checkMemory() {
		logger.info("Memory timeout: " + df.format(((double)runtime.totalMemory() - (double)runtime.freeMemory()) / (double)runtime.maxMemory() * 100) + "% running blockUntilFinished on current documents");
		logger.info("Used Mem: " + (runtime.totalMemory() - runtime.freeMemory()));
		logger.info("Free Mem: " + runtime.freeMemory());
		logger.info("Total Mem: " + runtime.totalMemory());
		logger.info("Max Memory: " + runtime.maxMemory());
	}
	
	public long addResultSet(ResultSet rs, int fetchSize) throws SQLException, SolrServerException, IOException {
		Date startTime = new Date();
		long count = 0;
		int innerCount = 0;
		Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
		ResultSetMetaData rsm = rs.getMetaData();
		int numColumns = rsm.getColumnCount();
		String[] colNames = new String[numColumns + 1];

		for (int i = 1; i < (numColumns + 1); i++) {
			colNames[i] = rsm.getColumnName(i).toLowerCase();
		}

		while (rs.next()) {
			count++;
			innerCount++;

			SolrInputDocument doc = new SolrInputDocument();

			/**
			 * At this point, take care of manual document field assignments for
			 * which you previously assigned the colNames entry to null.
			 */

			for (int j = 1; j < (numColumns + 1); j++) {
				if (colNames[j] != null) {
					Object f;
					switch (rsm.getColumnType(j)) {
					case Types.BIGINT: {
						f = rs.getLong(j);
						break;
					}
					case Types.INTEGER: {
						f = rs.getInt(j);
						break;
					}
					case Types.DATE: {
						f = rs.getDate(j);
						break;
					}
					case Types.FLOAT: {
						f = rs.getFloat(j);
						break;
					}
					case Types.DOUBLE: {
						f = rs.getDouble(j);
						break;
					}
					case Types.TIME: {
						f = rs.getDate(j);
						break;
					}
					case Types.BOOLEAN: {
						f = rs.getBoolean(j);
						break;
					}
					default: {
						f = rs.getString(j);
					}
					}
					doc.addField(colNames[j], f);
				}
			}
			docs.add(doc);

			/**
			 * When we reach fetchSize, index the documents and reset the inner
			 * counter.
			 */
			if (innerCount == fetchSize) {
				checkMemory();
				server.add(docs, 15000);
				docs.clear();
				innerCount = 0;
			}
		}

		/**
		 * If the outer loop ended before the inner loop reset, index the
		 * remaining documents.
		 */
		if (innerCount != 0) {
			server.add(docs, 15000);
			docs.clear();
		}
		Date now = new Date();
		logger.info("### Time: "+(now.getTime() - startTime.getTime())+"");
		return count;
	}
	
	public void index() {
		
		Connection con = null;
		Statement stmt = null;
		ResultSet rs = null;
		try {
			
			Class.forName(ResourceBundle.getBundle("edu.berkeley.urel.solr.config").getString("driver"));
			String connString = ResourceBundle.getBundle("edu.berkeley.urel.solr.config").getString("jdbcurl");
			con = DriverManager.getConnection(connString);
			stmt = con.createStatement();
			rs = stmt.executeQuery(query);
			
			addResultSet(rs, 5000);

		} catch (ClassNotFoundException cnfe) {
			logger.error(ExceptionUtils.getFullStackTrace(cnfe));
		} catch (SQLException sqle) {
			logger.error(ExceptionUtils.getFullStackTrace(sqle));
		} catch (SolrServerException sse) {
			logger.error(ExceptionUtils.getFullStackTrace(sse));
		} catch (IOException ioe) {
			logger.error(ExceptionUtils.getFullStackTrace(ioe));
		} finally {
			try {
				rs.close();
				stmt.close();
				con.close();
			} catch (SQLException sqle) {
				logger.error(ExceptionUtils.getFullStackTrace(sqle));
			}
		}

	}
	
	@Override
	public void run() {
		super.run();
		System.out.println("query: "+query);
		index();
	}
}
