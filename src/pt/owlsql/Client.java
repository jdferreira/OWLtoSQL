package pt.owlsql;

import java.io.IOException;
import java.sql.Connection;

import pt.json.JSONException;
import pt.owlsql.config.JSONConfig;


public class Client {
    
    private static Connection connection;
    
    
    public static void connect(String configFilename) throws IOException, JSONException {
        
        // Read the configuration file
        JSONConfig.read(configFilename);
        
        // Connect to the database and instantiate the necessary extractors
        connection = Extractor.connect();
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
