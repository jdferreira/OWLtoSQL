package pt.owlsql;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map.Entry;

import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;
import org.semanticweb.owlapi.model.OWLOntologyManager;

import pt.json.JSONException;
import pt.owlsql.config.JSONConfig;

import com.google.gson.JsonElement;


public class LoadAndExtract {
    
    // To load ontologies
    private static final OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
    
    // Hold on to the list of extractors and ontologies given as command line arguments
    private static final ArrayList<Extractor> extractors = new ArrayList<>();
    private static final HashSet<OWLOntology> entryPointOntologies = new HashSet<>();
    
    // Whether the database should be constructed from the ground up
    private static boolean fullWipe;
    
    private static ArrayList<String> argumentOntologies;
    
    private static ArrayList<String> argumentExtractorClassNames;
    
    
    private static void exit(String message) {
        System.err.println(message);
        System.exit(1);
    }
    
    
    private static void extractAll(HashSet<OWLOntology> ontologies) {
        // We start by processing the options of all the extractors, and only then do we execute each one of them
        // This ensures that errors are caught early in the game
        for (Extractor extractor : extractors) {
            try {
                System.out.println("Extracting information using " + extractor.getClass().getName());
                if (extractor instanceof Cacher)
                    ((Cacher) extractor).cache();
                else if (extractor instanceof OWLExtractor)
                    ((OWLExtractor) extractor).extract(ontologies);
                extractor.prepare();
            }
            catch (Exception e) {
                System.err.println("Cannot extract with " + extractor.getClass() + "\n");
                e.printStackTrace();
                // We break cause subsequent extractors may need the result of this
                break;
            }
        }
    }
    
    
    private static void getExtractors() {
        ArrayList<Class<? extends Extractor>> extractorClasses;
        
        if (argumentExtractorClassNames.size() == 0)
            extractorClasses = JSONConfig.getExtractorClasses();
        else {
            // Convert the given names into actual classes
            extractorClasses = new ArrayList<>();
            for (String className : argumentExtractorClassNames) {
                Class<?> raw;
                try {
                    // Do not initialize the class just yet
                    raw = Class.forName(className, false, ClassLoader.getSystemClassLoader());
                }
                catch (ClassNotFoundException e) {
                    exit("Unable to find class " + className);
                    throw new RuntimeException("Code does not reach this point!");
                }
                
                if (raw.equals(Extractor.class)
                        || raw.equals(OWLExtractor.class)
                        || raw.equals(Cacher.class)
                        || (!OWLExtractor.class.isAssignableFrom(raw) && Cacher.class.isAssignableFrom(raw)))
                    exit("Class "
                            + className
                            + " does not implement "
                            + OWLExtractor.class.getName()
                            + " nor "
                            + Cacher.class.getName());
                
                extractorClasses.add(raw.asSubclass(Extractor.class));
            }
        }
        
        for (int i = 0; i < extractorClasses.size(); i++) {
            Class<? extends Extractor> cls = extractorClasses.get(i);
            Extractor extractor;
            try {
                extractor = Extractor.getExtractor(cls);
            }
            catch (SQLException e) {
                System.err.println("Cannot get an instance the class " + cls.getName());
                e.printStackTrace();
                break;
            }
            
            // Process its options
            HashSet<String> mandatoryParameters = new HashSet<>();
            for (String parameterName : extractor.getMandatoryOptions()) {
                mandatoryParameters.add(parameterName);
            }
            
            for (Entry<String, JsonElement> entry : JSONConfig.getParameters(cls).entrySet()) {
                try {
                    extractor.processOption(entry.getKey(), entry.getValue());
                    mandatoryParameters.remove(entry.getKey());
                }
                catch (JSONException e) {
                    exit(e.withPrefix("extractors", "[" + i + "]", entry.getKey()).getMessage());
                }
            }
            
            if (mandatoryParameters.size() > 0)
                exit("Failed to provide option \""
                        + mandatoryParameters.iterator().next()
                        + "\" to extractor "
                        + cls.getName());
            
            extractors.add(extractor);
        }
    }
    
    
    private static void getOntologies() {
        ArrayList<URI> uris;
        
        if (argumentOntologies.size() == 0)
            uris = JSONConfig.getOntologiesURI();
        else {
            uris = new ArrayList<>();
            for (String ontologyURI : argumentOntologies) {
                try {
                    uris.add(new URI(ontologyURI));
                }
                catch (URISyntaxException e) {
                    exit(e.getMessage());
                }
            }
        }
        
        entryPointOntologies.addAll(loadOntologies(uris));
    }
    
    
    private static HashSet<OWLOntology> loadOntologies(ArrayList<URI> uris) {
        System.out.println("Loading ontologies ...");
        
        HashSet<OWLOntology> result = new HashSet<>();
        for (URI uri : uris) {
            IRI iri = IRI.create(uri);
            System.out.println("  " + uri);
            OWLOntology ontology;
            try {
                ontology = manager.loadOntology(iri);
            }
            catch (OWLOntologyCreationException e) {
                exit(e.getMessage());
                return null;
            }
            
            if (ontology.isAnonymous())
                exit("Anonymous ontologies are not compatible with this program.");
            
            result.add(ontology);
        }
        
        return result;
    }
    
    
    private static void processArguments(String[] args) {
        String configFilename = JSONConfig.CONFIG_FILE;
        
        argumentOntologies = new ArrayList<>();
        argumentExtractorClassNames = new ArrayList<>();
        
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("-c") || args[i].equals("--config")) {
                i++;
                configFilename = args[i];
            }
            else if (args[i].equals("-o") || args[i].equals("--ontology")) {
                i++;
                argumentOntologies.add(args[i]);
            }
            else if (args[i].equals("-x") || args[i].equals("--extractor")) {
                i++;
                argumentExtractorClassNames.add(args[i]);
            }
            else
                exit("Unrecognized command line argument " + args[i]);
        }
        
        if (argumentOntologies.size() > 0 && argumentExtractorClassNames.size() == 0)
            exit("You cannot specify an ontology unless you specify at least one extractor too");
        fullWipe = argumentExtractorClassNames.size() == 0;
        
        try {
            JSONConfig.read(configFilename);
        }
        catch (JSONException | IOException e) {
            exit(e.getMessage());
        }
    }
    
    
    @SuppressWarnings("resource")
    private static void setupTables() {
        Connection connection = Extractor.getConnection();
        try {
            Statement statement = connection.createStatement();
            statement.execute(""
                    + "CREATE TABLE IF NOT EXISTS ontologies ("
                    + "id INT PRIMARY KEY AUTO_INCREMENT, "
                    + "ontology_iri TEXT NOT NULL,"
                    + "version_iri TEXT NOT NULL)");
            statement.close();
            
            PreparedStatement select = connection.prepareStatement(""
                    + "SELECT COUNT(*) FROM ontologies WHERE ontology_iri = ? AND version_iri = ?");
            PreparedStatement insert = connection.prepareStatement(""
                    + "INSERT INTO ontologies (ontology_iri, version_iri) "
                    + "VALUES (?, ?)");
            for (OWLOntology ontology : entryPointOntologies) {
                // Determine if the ontology had already been loaded
                String stringOntologyIRI = ontology.getOntologyID().getOntologyIRI().toString();
                IRI versionIRI = ontology.getOntologyID().getVersionIRI();
                String stringVersionIRI = versionIRI == null ? "" : versionIRI.toString();
                
                select.setString(1, stringOntologyIRI);
                select.setString(2, stringVersionIRI);
                try (ResultSet resultSet = select.executeQuery()) {
                    resultSet.next();
                    if (resultSet.getInt(1) == 0) {
                        insert.setString(1, stringOntologyIRI);
                        insert.setString(2, stringVersionIRI);
                        insert.executeUpdate();
                    }
                }
            }
            select.close();
            insert.close();
        }
        catch (SQLException e) {
            System.err.println("Unable to create fundamental tables in the database");
            e.printStackTrace();
            System.exit(1);
        }
    }
    
    
    private static void wipeDatabase() {
        // We connect to the database as the OWLExtractor.connect() method does
        String host = JSONConfig.getHost();
        String database = JSONConfig.getDatabase();
        String username = JSONConfig.getUsername();
        String password = JSONConfig.getPassword();
        
        // Setup the connection with the DB
        String uri = "jdbc:mysql://" + host + "/?user=" + username + "&password=" + password;
        try (Connection connection = DriverManager.getConnection(uri);
                Statement statement = connection.createStatement()) {
            try {
                statement.executeUpdate("DROP DATABASE IF EXISTS " + database);
            }
            catch (SQLException e) {
                exit("Unable to wipe the database:\n" + e.getMessage());
            }
            try {
                statement.executeUpdate("CREATE DATABASE " + database);
            }
            catch (SQLException e) {
                exit("Unable to recreate the database:\n" + e.getMessage());
            }
        }
        catch (SQLException e) {
            exit("Cannot access the database:\n" + e.getMessage());
        }
        
    }
    
    
    public static void main(String[] args) {
        // This will load the MySQL driver
        try {
            Class.forName("com.mysql.jdbc.Driver");
        }
        catch (Exception e) {
            exit("Unable to load the MySQL Driver");
        }
        
        processArguments(args);
        
        Extractor.connect(); // Connect to the database and instantiate the necessary extractors
        
        getExtractors();
        getOntologies();
        
        if (fullWipe)
            wipeDatabase();
        
        setupTables(); // Setup fundamental tables in the database
        extractAll(entryPointOntologies); // Extract all the information
        Extractor.closeConnection(); // Close the connection to the database
    }
}
