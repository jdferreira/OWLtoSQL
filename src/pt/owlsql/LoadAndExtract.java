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
import java.util.Collections;
import java.util.HashSet;

import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;
import org.semanticweb.owlapi.model.OWLOntologyManager;

import pt.json.JSONException;
import pt.owlsql.config.ExtractorSpec;
import pt.owlsql.config.JSONConfig;


public class LoadAndExtract {
    
    // To load ontologies
    private static final OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
    
    // Hold on to the list of extractors and ontologies given as command line arguments
    private static final ArrayList<Extractor> extractors = new ArrayList<>();
    private static final HashSet<OWLOntology> entryPointOntologies = new HashSet<>();
    
    // Whether the database should be constructed from the ground up
    private static boolean fullWipe;
    
    private static ArrayList<String> argumentOntologies;
    
    private static ArrayList<Integer> toRun;
    
    
    private static void exit(String message, Throwable e) {
        System.err.println(message);
        if (e != null)
            e.printStackTrace();
        System.exit(1);
    }
    
    
    private static void exit(String message) {
        exit(message, null);
    }
    
    
    private static void extractAll() {
        for (Extractor extractor : extractors) {
            try {
                System.out.println("Extracting information using " + extractor.getClass().getName());
                if (extractor instanceof Cacher)
                    ((Cacher) extractor).cache();
                else if (extractor instanceof OWLExtractor)
                    ((OWLExtractor) extractor).extract(entryPointOntologies);
            }
            catch (Exception e) {
                exit("Cannot extract with " + extractor.getClass(), e);
            }
        }
    }
    
    
    private static void getExtractors() {
        ArrayList<ExtractorSpec<?>> specs = JSONConfig.getExtractorSpecs();
        for (int index : toRun) {
            try {
                extractors.add(Extractor.createFromSpec(specs.get(index)));
            }
            catch (InstantiationException | IllegalAccessException e) {
                exit("Cannot instantiate extractor on position " + index, e);
            }
            catch (JSONException e) {
                exit(e.withPrefix("extractors", "[" + index + "]").getMessage());
            }
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
                throw new RuntimeException("Code does not reach this point!");
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
        toRun = null;
        
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("-c") || args[i].equals("--config")) {
                i++;
                configFilename = args[i];
            }
            else if (args[i].equals("-o") || args[i].equals("--ontology")) {
                i++;
                argumentOntologies.add(args[i]);
            }
            else if (args[i].equals("-x") || args[i].equals("--index")) {
                i++;
                toRun = getIndices(args[i]);
            }
            else
                exit("Unrecognized command line argument " + args[i]);
        }
        
        fullWipe = toRun == null;
        
        try {
            JSONConfig.read(configFilename);
        }
        catch (JSONException | IOException e) {
            exit(e.getMessage());
        }
        
        if (toRun == null) {
            toRun = new ArrayList<>();
            ArrayList<ExtractorSpec<?>> specs = JSONConfig.getExtractorSpecs();
            for (int i = 0; i < specs.size(); i++) {
                toRun.add(i);
            }
        }
    }
    
    
    private static ArrayList<Integer> getIndices(String string) {
        HashSet<Integer> set = new HashSet<>();
        
        // The argument must be a comma-separated sequence of ranges
        // where each range is either a single integer or two integers in ascending order separated by a hyphen.
        // Spaces are allowed and ignored (only leading and trailing!); everything else is an error
        String[] ranges = string.split(",");
        for (String range : ranges) {
            if (range.contains("-")) {
                // Is this a proper range?
                String[] ends = range.split("-");
                if (ends.length != 2)
                    exit("'" + range + "' is not a valid range: format is \"N-N\"");
                
                int start;
                int end;
                try {
                    start = Integer.parseInt(ends[0].trim());
                    end = Integer.parseInt(ends[1].trim());
                }
                catch (NumberFormatException e) {
                    exit("'" + range + "' is not a valid range: format is \"N-N\"");
                    throw new RuntimeException("Code does not reach this point!");
                }
                
                if (start >= end)
                    exit("'" + range + "' is not a valid range: ends are not in ascending order");
                
                for (int i = start; i <= end; i++) {
                    set.add(i);
                }
            }
            else {
                int num;
                try {
                    num = Integer.parseInt(range.trim());
                    set.add(num);
                }
                catch (NumberFormatException e) {
                    exit("'" + range + "' is not a valid number");
                }
            }
        }
        
        ArrayList<Integer> result = new ArrayList<>(set);
        Collections.sort(result);
        return result;
    }
    
    
    @SuppressWarnings("resource")
    private static void setupTables() {
        Connection connection = Extractor.getConnection();
        try {
            Statement statement = connection.createStatement();
            statement.execute(""
                    + "CREATE TABLE IF NOT EXISTS ontologies ("
                    + "  id INT PRIMARY KEY AUTO_INCREMENT, "
                    + "  ontology_iri TEXT NOT NULL,"
                    + "  version_iri TEXT NOT NULL)");
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
            exit("Unable to create fundamental tables in the database", e);
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
                exit("Unable to wipe the database:", e);
            }
            try {
                statement.executeUpdate("CREATE DATABASE " + database);
            }
            catch (SQLException e) {
                exit("Unable to recreate the database:", e);
            }
        }
        catch (SQLException e) {
            exit("Cannot access the database:", e);
        }
        
    }
    
    
    public static void main(String[] args) {
        // This will load the MySQL driver
        try {
            Class.forName("com.mysql.jdbc.Driver");
        }
        catch (Exception e) {
            exit("Unable to load the MySQL Driver", e);
        }
        
        processArguments(args);
        
        Extractor.connect(); // Connect to the database and instantiate the necessary extractors
        
        getExtractors();
        getOntologies();
        
        if (fullWipe)
            wipeDatabase();
        
        // TODO What this should do is this: If the set of loaded ontologies is different from the set of ontologies
        // already inlcuded in the database, a full wipe should be performed!
        setupTables(); // Setup fundamental tables in the database
        
        extractAll(); // Extract all the information
        Extractor.closeConnection(); // Close the connection to the database
    }
}
