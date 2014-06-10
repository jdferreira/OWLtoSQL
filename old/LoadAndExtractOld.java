package pt.owlsql;

import java.net.URI;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;

import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;
import org.semanticweb.owlapi.model.OWLOntologyID;
import org.semanticweb.owlapi.model.OWLOntologyManager;


public class LoadAndExtractOld {
    
    private static final OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
    private static final HashSet<Class<? extends OWLExtractor>> forcedExtractors = new HashSet<>();
    
    private static boolean allExtractors;
    private static String configFilename;
    
    
    private static HashSet<Class<? extends OWLExtractor>> getAlreadyExtracted(HashSet<OWLOntology> loadedOntologies)
            throws SQLException {
        HashSet<Class<? extends OWLExtractor>> result = new HashSet<>();
        
        // If the ontologies loaded this time are not the same as the ones loaded to populate the database, we must
        // rerun all the extractors. This identification is made through the ontology ID
        HashSet<OWLOntologyID> loadedOntologiesID = new HashSet<>();
        for (OWLOntology ontology : loadedOntologies) {
            loadedOntologiesID.add(ontology.getOntologyID());
        }
        
        try (Statement statement = OWLExtractor.getConnection().createStatement()) {
            // Get the ontology ID's stored in the database. They must match the ones of the ontologies loaded, or else
            // all the extractors will execute.
            HashSet<OWLOntologyID> savedOntologiesID = new HashSet<>();
            try (ResultSet resultSet = statement.executeQuery("SELECT ontology_iri, version_iri FROM ontologies")) {
                while (resultSet.next()) {
                    IRI ontologyIRI = IRI.create(resultSet.getString(1));
                    String versionIRIString = resultSet.getString(2);
                    if (versionIRIString.equals("")) {
                        savedOntologiesID.add(new OWLOntologyID(ontologyIRI));
                    }
                    else {
                        IRI versionIRI = IRI.create(versionIRIString);
                        savedOntologiesID.add(new OWLOntologyID(ontologyIRI, versionIRI));
                    }
                }
            }
            
            // Now put the loaded ontologies in the database
            try (PreparedStatement insert = OWLExtractor.getConnection()
                    .prepareStatement("INSERT INTO ontologies (ontology_iri, version_iri) VALUE (?, ?)")) {
                for (OWLOntologyID ontologyID : loadedOntologiesID) {
                    if (savedOntologiesID.contains(ontologyID))
                        continue; // Don't store duplicate entries in this table
                        
                    String ontologyIRIString = ontologyID.getOntologyIRI().toString();
                    IRI versionIRI = ontologyID.getVersionIRI();
                    String versionIRIString = versionIRI == null ? "" : versionIRI.toString();
                    insert.setString(1, ontologyIRIString);
                    insert.setString(2, versionIRIString);
                    insert.executeUpdate();
                }
            }
            
            // Different ontologies! All extractors must be run from scratch
            if (!loadedOntologiesID.equals(savedOntologiesID)) {
                statement.close();
                return new HashSet<>();
            }
            
            // Now that we know the ontologies are the same, let's determine the extractors that have succeeded in the
            // past
            try (ResultSet resultSet = statement.executeQuery("SELECT * FROM extractors_used")) {
                while (resultSet.next()) {
                    try {
                        Class<? extends OWLExtractor> cls = (Class<? extends OWLExtractor>) Class.forName(resultSet
                                .getString(1));
                        int currentVersion = OWLExtractor.getExtractor(cls).getVersion();
                        int oldVersion = resultSet.getInt(2);
                        if (oldVersion == currentVersion)
                            result.add(cls);
                    }
                    catch (ClassCastException | ClassNotFoundException e) {
                        result.clear();
                        throw new SQLException(e.getMessage());
                    }
                }
            }
        }
        
        // Remove the extractors that the user specifically requested
        result.removeAll(forcedExtractors);
        return result;
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
    
    
    @SuppressWarnings("unchecked")
    private static void processArguments(String[] args) throws ConfigException {
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("-X"))
                allExtractors = true;
            else if (args[i].equals("-x")) {
                i++;
                Class<?> cls;
                try {
                    // Do not initialize the class just yet
                    cls = Class.forName(args[i], false, ClassLoader.getSystemClassLoader());
                }
                catch (ClassNotFoundException e) {
                    throw new ConfigException("Unable to find class " + args[i]);
                }
                if (cls.equals(OWLExtractor.class) || !OWLExtractor.class.isAssignableFrom(cls))
                    throw new ConfigException("Class " + args[i] + " does not implement " + OWLExtractor.class);
                forcedExtractors.add((Class<? extends OWLExtractor>) cls);
            }
            else if (args[i].equals("-c")) {
                i++;
                configFilename = args[i];
            }
            else
                throw new ConfigException("Unrecognized command line argument " + args[i]);
        }
    }
    
    
    private static void setupTables() {
        try (Statement statement = OWLExtractor.getConnection().createStatement()) {
            statement.execute(""
                    + "CREATE TABLE IF NOT EXISTS ontologies ("
                    + "id INT PRIMARY KEY AUTO_INCREMENT, "
                    + "ontology_iri TEXT,"
                    + "version_iri TEXT)");
            statement.execute("CREATE TABLE IF NOT EXISTS extractors_used ("
                    + "java_class_name VARCHAR(256), "
                    + "version INT, "
                    + "UNIQUE (java_class_name))");
        }
        catch (SQLException e) {
            System.err.println("Unable to create fundamental tables in the database");
            e.printStackTrace();
            System.exit(1);
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
    
    
    static void extractAll(HashSet<OWLOntology> ontologies) {
        // Determine if we must extract the information with all the extractors or if some have already been used and
        // their information stored in the database
        HashSet<Class<? extends OWLExtractor>> alreadyExtracted;
        try {
            alreadyExtracted = getAlreadyExtracted(ontologies);
        }
        catch (SQLException e) {
            System.err.println("Error on reading which extractors have already been used.");
            e.printStackTrace();
            return;
        }
        
        for (Class<? extends OWLExtractor> cls : ConfigReader.getExtractorClasses()) {
            OWLExtractor extractor = OWLExtractor.getExtractor(cls);
            try {
                if (!alreadyExtracted.contains(cls)) {
                    System.out.println("Extracting information using " + cls);
                    extractor.extract(ontologies);
                    storeExtractorUsed(extractor);
                }
                extractor.prepare();
            }
            catch (SQLException e) {
                System.err.println("Cannot extract the information of the ontologies with the instance of "
                        + extractor.getClass());
                e.printStackTrace();
                break;
            }
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
            
            if (allExtractors)
                forcedExtractors.addAll(ConfigReader.getExtractorClasses());
            
            entryPointOntologies = loadOntologies();
        }
        catch (ConfigException e) {
            System.err.println("ERROR: " + e.getMessage());
            return;
        }
        
        // Connect to the database and instantiate the necessary extractors
        OWLExtractor.connect();
        
        // Setup fundamental tables in the database
        setupTables();
        
        // Extract all the information
        extractAll(entryPointOntologies);
        
        // Close the connection to the database
        OWLExtractor.close();
    }
}
