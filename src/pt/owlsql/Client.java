package pt.owlsql;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import pt.json.JSONException;
import pt.owlsql.config.JSONConfig;


public class Client {
    
    private static Connection connection;
    
    
    public static void connect(String configFilename) throws SQLException, IOException, JSONException {
        // Read the configuration file
        JSONConfig.read(configFilename);
        
        // Connect to the database and instantiate the necessary extractors
        connection = Extractor.connect();
        
        // And then prepare all the extractors for use
        for (Class<? extends Extractor> cls : JSONConfig.getExtractorClasses()) {
            Extractor extractor = Extractor.getExtractor(cls);
            try {
                extractor.prepare();
            }
            catch (SQLException e) {
                System.err.println("Cannot extract the information of the ontologies with the instance of "
                        + extractor.getClass());
                e.printStackTrace();
            }
        }
    }
    
    
    public static Connection getConnection() {
        if (connection != null)
            return connection;
        else
            throw new RuntimeException("Not connected to the database");
    }
    
    
    public static void closeConnection() {
        Extractor.closeConnection();
    }
}
