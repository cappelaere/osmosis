// This software is released into the Public Domain.  See copying.txt for details.
package org.openstreetmap.osmosis.apidb.common;

import org.apache.commons.dbcp.BasicDataSource;
import org.openstreetmap.osmosis.core.OsmosisRuntimeException;
import org.openstreetmap.osmosis.core.database.DatabaseLoginCredentials;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Produces data sources based on a set of database credentials.
 */
public final class DataSourceFactory {
    private static final Logger LOG = Logger.getLogger(DataSourceFactory.class.getName());
	
	/**
	 * This class cannot be instantiated.
	 */
	private DataSourceFactory() {
	}
	
	
	/**
	 * Creates a new data source based on the specified credentials.
	 * 
	 * @param credentials
	 *            The database credentials.
	 * 
	 * @return The data source.
	 */
	public static BasicDataSource createDataSource(DatabaseLoginCredentials credentials) {
		BasicDataSource dataSource;
		
		dataSource = new BasicDataSource();
		
        switch (credentials.getDbType()) {
        case POSTGRESQL:			
			
        	dataSource.setDriverClassName("org.postgresql.Driver");
			String url = "jdbc:postgresql://" + credentials.getHost() + "/" + credentials.getDatabase();
			if ( credentials.getSsl()) {
				url += "?ssl=true";
				if( credentials.getSslFactory() != null ) {
					url +="&sslfactory="+credentials.getSslFactory();
				}
			}
			
			LOG.finest("Connecting to url:"+url);
				
        	dataSource.setUrl(url);
        	break;
        case MYSQL:
        	dataSource.setDriverClassName("com.mysql.jdbc.Driver");
        	dataSource.setUrl("jdbc:mysql://" + credentials.getHost() + "/" + credentials.getDatabase());
            break;
        default:
            throw new OsmosisRuntimeException("Unknown database type " + credentials.getDbType() + ".");
        }
        
        dataSource.setUsername(credentials.getUser());
        dataSource.setPassword(credentials.getPassword());
        
        return dataSource;
	}
}
