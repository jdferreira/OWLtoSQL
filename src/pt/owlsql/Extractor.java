package pt.owlsql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
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
    
    /**
     * Counts the number of rows in a {@link ResultSet}. This method implies moving the pointer of the result set around
     * (which means that it is not thread safe). Additionally, the pointer is moved back to its initial position at the
     * end of the method. However, this repositioning may fail, changing the state of the set.
     * 
     * @param resultSet The {@link ResultSet} whose rows will be counted.
     * 
     * @return The number of rows in the given result set.
     * 
     * @throws SQLException if a database access error occurs or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support {@link ResultSet#getRow()},
     *             {@link ResultSet#last()} or {@link ResultSet#absolute(int)}.
     */
    protected static int countRows(ResultSet resultSet) throws SQLException {
        int current = resultSet.getRow();
        resultSet.last();
        // Note: the previous lines will throw a SQLFeatureNotSupportedException if the driver does not allow jumping
        // around. If it fails, the state of the set is not changed. However, if the next lines fail, we need to restore
        // the initial state
        
        try {
            return resultSet.getRow();
        }
        finally {
            // No matter what happens, return to initial state
            resultSet.absolute(current);
        }
    }
    
    
    protected static Connection getConnection() {
        return connection;
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
        
        return connection;
    }
    
    
    static <U extends Extractor> U createFromSpec(ExtractorSpec<U> spec) throws InstantiationException,
            IllegalAccessException, JSONException {
        Class<U> cls = spec.getExtractorClass();
        Hashtable<String, JsonElement> parameters = spec.getParameters();
        
        if (parameters.isEmpty()) {
            @SuppressWarnings("unchecked")
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
            
            throw new InstantiationException("Failed to provide " + options + " to extractor " + cls.getName());
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
    
    
    public static <U extends Extractor> U getExtractor(Class<U> cls) {
        @SuppressWarnings("unchecked")
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
    
    
    private static Connection connection;
    
    
    private static Hashtable<Class<? extends Extractor>, Extractor> simpleInstances = new Hashtable<>();
    
    
    protected static final OWLDataFactory factory = OWLManager.getOWLDataFactory();
    
    
    @SuppressWarnings("static-method")
    protected String[] getMandatoryOptions() {
        return new String[0];
    }
    
    
    protected abstract void prepare() throws SQLException;
    
    
    protected void processOption(String key, @SuppressWarnings("unused") JsonElement element) throws JSONException {
        throw new JSONException("unexpected parameter on " + getClass().getName(), key);
    }
}
