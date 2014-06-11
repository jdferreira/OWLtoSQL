package pt.owlsql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Map.Entry;

import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.OWLDataFactory;

import pt.json.JSONException;
import pt.owlsql.config.ExtractorSpec;
import pt.owlsql.config.JSONConfig;

import com.google.gson.JsonElement;


public abstract class Extractor {
    
    private static Connection connection;
    private static Hashtable<Class<? extends Extractor>, Extractor> simpleInstances = new Hashtable<>();
    
    protected static final OWLDataFactory factory = OWLManager.getOWLDataFactory();
    
    
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
        
        return connection;
    }
    
    
    protected static Connection getConnection() {
        return connection;
    }
    
    
    public static <U extends Extractor> U getExtractor(Class<U> cls) {
        U cached = (U) simpleInstances.get(cls);
        if (cached != null)
            return cached;
        
        U extractor;
        try {
            extractor = cls.newInstance();
        }
        catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
        
        if (extractor.getMandatoryOptions().length > 0)
            throw new RuntimeException("Extractors of type "
                    + cls.getName()
                    + " mandate a set of parameters. They cannot be created using this method.");
        
        try {
            extractor.prepare();
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
        
        simpleInstances.put(cls, extractor);
        return extractor;
    }
    
    
    protected abstract void prepare() throws SQLException;
    
    
    @SuppressWarnings("static-method")
    protected String[] getMandatoryOptions() {
        return new String[0];
    }
    
    
    protected void processOption(String key, @SuppressWarnings("unused") JsonElement element) throws JSONException {
        throw new JSONException("unexpected parameter on " + getClass().getName(), key);
    }
    
    
    static <U extends Extractor> U createFromSpec(ExtractorSpec<U> spec) throws InstantiationException,
            IllegalAccessException, JSONException {
        Class<U> cls = spec.getExtractorClass();
        Hashtable<String, JsonElement> parameters = spec.getParameters();
        
        if (parameters.isEmpty()) {
            U cached = (U) simpleInstances.get(cls);
            if (cached != null)
                return cached;
        }
        
        U extractor = cls.newInstance();
        
        // Process its options
        HashSet<String> mandatoryParameters = new HashSet<>();
        for (String parameterName : extractor.getMandatoryOptions()) {
            mandatoryParameters.add(parameterName);
        }
        
        for (Entry<String, JsonElement> entry : parameters.entrySet()) {
            String key = entry.getKey();
            extractor.processOption(key, entry.getValue());
            mandatoryParameters.remove(key);
        }
        
        if (mandatoryParameters.size() > 0) {
            StringBuilder sb = new StringBuilder();
            String[] params = mandatoryParameters.toArray(new String[mandatoryParameters.size()]);
            for (int i = 0; i < params.length - 1; i++) {
                sb.append(", \"").append(params[i]).append("\"");
            }
            
            String options;
            if (params.length == 1)
                options = "option \"" + params[0] + "\"";
            else
                options = "options " + sb.substring(2) + " and \"" + params[params.length - 1] + "\"";
            
            throw new InstantiationException("Failed to provide "
                    + options
                    + " to extractor "
                    + cls.getName());
        }
        
        try {
            extractor.prepare();
        }
        catch (SQLException e) {
            throw new InstantiationException("Unable to prepare an instance of the class " + cls.getName());
        }
        
        // If no parameters are given, we can cache this extractor
        if (parameters.isEmpty())
            simpleInstances.put(cls, extractor);
        
        return extractor;
    }
}
