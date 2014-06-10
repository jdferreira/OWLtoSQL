package pt.owlsql;

import java.net.URI;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Hashtable;

import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;
import org.semanticweb.owlapi.model.OWLOntologyID;
import org.semanticweb.owlapi.model.OWLOntologyManager;


public class LoadAndExtract {
    
    private static final OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
    
    private static boolean wipeDatabase;
    private static String configFilename;
    
    
    private static void extractAll(HashSet<OWLOntology> ontologies) {
        if (wipeDatabase)
            wipeDatabase();
        
        // Let's read from the database the tuples (OWLOntologyID, OWLExtractor, version) that have already been used
        // to extract information into the database itself, and store them in an easy-access data structure
        // This will be empty if the database has been previously wiped
        Hashtable<OWLOntologyID, Hashtable<Class<? extends OWLExtractor>, Integer>> alreadyUsed = getAlreadyUsed();
        
        // Now, for each ontology and each extractor class, run the extractor on that ontology
        for (OWLOntology ontology : ontologies) {
            OWLOntologyID ontologyID = ontology.getOntologyID();
            System.out.println("Ontology = " + ontologyID);
            Hashtable<Class<? extends OWLExtractor>, Integer> versions = alreadyUsed.get(ontologyID);
            if (versions == null)
                alreadyUsed.put(ontologyID, versions = new Hashtable<>());
            for (Class<? extends OWLExtractor> extractorClass : ConfigReader.getExtractorClasses()) {
                OWLExtractor extractor = OWLExtractor.getExtractor(extractorClass);
                if (versions.containsKey(extractorClass) && versions.get(extractorClass) == extractor.getVersion())
                    // This ontology has been used with this exact version of the extractor. Nothing to be done here
                    continue;
                
                System.out.println("  Extractor = " + extractorClass);
                try {
                    extractor.extract(ontology);
                    extractor.prepare();
                    storeExtractorUsed(extractor);
                }
                catch (SQLException e) {
                    System.err.println("    ERROR");
                    e.printStackTrace();
                    System.exit(1);
                    return;
                }
            }
        }
    }
    
    
    private static void wipeDatabase() {
        String state = "pre-wipe";
        try (Statement statement = OWLExtractor.getConnection().createStatement()) {
            String database = ConfigReader.getDatabase();
            statement.executeUpdate("DROP DATABASE " + database);
            state = "pre-create";
            statement.executeUpdate("CREATE DATABASE " + database);
            state = "pre-use";
            statement.executeUpdate("USE " + database);
        }
        catch (Exception e) {
            if (state.equals("pre-wipe"))
                System.err.println("Unable to wipe the database.");
            else if (state.equals("pre-create"))
                System.err.println("Unable to create the empty database.");
            else if (state.equals("pre-use"))
                System.err.println("Unable to go into the newly created database.");
            e.printStackTrace();
            System.exit(1);
            return;
        }
    }
    
    
    private static HashSet<OWLOntology> loadOntologies() throws ConfigException {
        System.out.println("Loading ontologies ...");
        
        HashSet<OWLOntology> result = new HashSet<>();
        for (URI uri : ConfigReader.getOntologiesURI()) {
            IRI iri = IRI.create(uri);
            System.out.println("  " + uri);
            OWLOntology ontology;
            try {
                ontology = manager.loadOntology(iri);
            }
            catch (OWLOntologyCreationException e) {
                throw new ConfigException("Unable to load an OWL ontology from " + uri);
            }
            
            if (ontology.isAnonymous())
                throw new ConfigException("Anonymous ontologies are not compatible with this program.");
            
            result.add(ontology);
        }
        
        return result;
    }
    
    
    private static void processArguments(String[] args) throws ConfigException {
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("-w") || args[i].equals("--wipe"))
                wipeDatabase = true;
            else if (args[i].equals("-c") || args[i].equals("--config")) {
                i++;
                configFilename = args[i];
            }
            else
                throw new ConfigException("Unrecognized command line argument " + args[i]);
        }
    }
    
    
    private static Hashtable<OWLOntologyID, Hashtable<Class<? extends OWLExtractor>, Integer>> getAlreadyUsed() {
        Hashtable<OWLOntologyID, Hashtable<Class<? extends OWLExtractor>, Integer>> result = new Hashtable<>();
        
        try (Statement statement = OWLExtractor.getConnection().createStatement()) {
            String query = ""
                    + "SELECT ontology_iri, version_iri, extractor_class, version "
                    + "FROM used_extractors "
                    + "JOIN ontologies ON ontologies.id = ontology_id";
            try (ResultSet resultSet = statement.executeQuery(query)) {
                while (resultSet.next()) {
                    OWLOntologyID ontologyID = getOntologyID(resultSet.getString(1), resultSet.getString(2));
                    Hashtable<Class<? extends OWLExtractor>, Integer> versions = result.get(ontologyID);
                    if (versions == null)
                        result.put(ontologyID, versions = new Hashtable<>());
                    
                    Class<?> raw = Class.forName(resultSet.getString(3));
                    if (raw == OWLExtractor.class || !OWLExtractor.class.isAssignableFrom(raw))
                        throw new ClassCastException(raw + " does not extend " + OWLExtractor.class.getName());
                    
                    @SuppressWarnings("unchecked")
                    Class<? extends OWLExtractor> cls = (Class<? extends OWLExtractor>) raw;
                    versions.put(cls, resultSet.getInt(4));
                }
            }
        }
        catch (Exception e) {
            if (e instanceof SQLException)
                System.err.println("Cannot find information on which extractions have already been performed");
            else if (e instanceof ClassNotFoundException)
                System.err.println("Found an unexpected extractor class");
            e.printStackTrace();
            System.exit(1);
            return null;
        }
        
        return result;
    }
    
    
    private static OWLOntologyID getOntologyID(String ontology, String version) {
        IRI ontologyIRI = IRI.create(ontology);
        if (version == null || version.equals(""))
            return new OWLOntologyID(ontologyIRI);
        return new OWLOntologyID(ontologyIRI, IRI.create(version));
    }
    
    
    private static void setupTables() {
        try (Statement statement = OWLExtractor.getConnection().createStatement()) {
            statement.execute(""
                    + "CREATE TABLE IF NOT EXISTS ontologies ("
                    + "id INT PRIMARY KEY AUTO_INCREMENT,"
                    + "ontology_iri TEXT NOT NULL,"
                    + "version_iri TEXT)");
            statement.execute("CREATE TABLE IF NOT EXISTS used_extractors ("
                    + "ontology_id INT NOT NULL,"
                    + "extractor_class VARCHAR(256) NOT NULL,"
                    + "version INT NOT NULL,"
                    + "UNIQUE (ontology_id, extractor_class),"
                    + "INDEX (extractor_class))");
        }
        catch (SQLException e) {
            System.err.println("Unable to create fundamental tables in the database");
            e.printStackTrace();
            System.exit(1);
            return;
        }
    }
    
    
    private static void storeExtractorUsed(OWLExtractor extractor) throws SQLException {
        try (PreparedStatement statement = OWLExtractor.getConnection()
                .prepareStatement("REPLACE INTO extractors_used (java_class_name, version) VALUES (?, ?)")) {
            statement.setString(1, extractor.getClass().getName());
            statement.setInt(2, extractor.getVersion());
            statement.executeUpdate();
        }
    }
    
    
    public static void main(String[] args) {
        
        // Read the configuration file and prepare for extraction
        HashSet<OWLOntology> entryPointOntologies;
        
        try {
            // Process the command line arguments
            processArguments(args);
            if (configFilename == null)
                configFilename = ConfigReader.CONFIG_FILE;
            
            ConfigReader.initialize(configFilename);
            entryPointOntologies = loadOntologies();
        }
        catch (ConfigException e) {
            System.err.println("ERROR: " + e.getMessage());
            return;
        }
        
        OWLExtractor.connect(); // Connect to the database
        setupTables(); // Setup fundamental tables in the database
        extractAll(entryPointOntologies); // Extract all the information
        OWLExtractor.close(); // Close the connection to the database
    }
}
