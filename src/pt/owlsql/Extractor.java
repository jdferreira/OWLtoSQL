package pt.owlsql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Hashtable;

import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.OWLDataFactory;

import pt.json.JSONException;
import pt.owlsql.config.JSONConfig;

import com.google.gson.JsonElement;


public abstract class Extractor {
    
    private static Connection connection;
    private static Hashtable<Class<? extends Extractor>, Extractor> instances = new Hashtable<>();
    private static HashSet<Extractor> prepared = new HashSet<>();
    
    protected static final OWLDataFactory factory = OWLManager.getOWLDataFactory();
    
    
    private static void instanciateSubclasses() {
        for (Class<? extends Extractor> cls : JSONConfig.getExtractorClasses()) {
            Extractor extractor;
            try {
                extractor = cls.newInstance();
            }
            catch (InstantiationException | IllegalAccessException e) {
                System.err.println("Cannot instantiate " + cls);
                e.printStackTrace();
                continue;
            }
            instances.put(cls, extractor);
        }
    }
    
    
    static void closeConnection() {
        try {
            connection.close();
        }
        catch (SQLException e) {
            System.err.println("Unable to close connection to the database");
        }
    }
    
    
    static Connection connect() {
        // Let's start by establishing a connection to MySQL
        String host = JSONConfig.getHost();
        String database = JSONConfig.getDatabase();
        String username = JSONConfig.getUsername();
        String password = JSONConfig.getPassword();
        
        // Setup the connection with the DB
        String uri = "jdbc:mysql://" + host + "/" + database + "?user=" + username + "&password=" + password;
        try {
            connection = DriverManager.getConnection(uri);
        }
        catch (SQLException e) {
            throw new Error("Unable to connect to the specified database", e);
        }
        
        instanciateSubclasses();
        
        return connection;
    }
    
    
    protected static Connection getConnection() {
        return connection;
    }
    
    
    public static <U extends Extractor> U getExtractor(Class<U> cls) throws SQLException {
        if (!instances.containsKey(cls))
            throw new IllegalArgumentException("Extractor " + cls.getName() + " has not been initialized yet");
        U result = (U) instances.get(cls);
        if (!prepared.contains(result)) {
            result.prepare();
            prepared.add(result);
        }
        
        return result;
    }
    
    
    protected abstract void prepare() throws SQLException;
    
    
    @SuppressWarnings("static-method")
    protected String[] getMandatoryOptions() {
        return new String[0];
    }
    
    
    protected void processOption(String key, JsonElement element) throws JSONException {
        throw new JSONException("unexpected parameter on " + getClass().getName(), key);
    }
    
}
