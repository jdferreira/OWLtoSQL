package pt.owlsql.extractors;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Set;

import org.semanticweb.owlapi.model.EntityType;
import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLEntity;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyID;

import pt.owlsql.OWLExtractor;


public final class SQLCoreUtils extends OWLExtractor {
    
    private static final Hashtable<String, EntityType<?>> nameToType = new Hashtable<>();
    
    static {
        for (EntityType<?> type : EntityType.values()) {
            nameToType.put(type.getName(), type);
        }
    }
    
    
    private static void extractEntityID(Hashtable<OWLEntity, Integer> done, OWLEntity entity,
            PreparedStatement indexNewEntity, PreparedStatement entityOntologyAssociation) throws SQLException {
        
        // Store (or retrieve) the internal ID of this entity to associate it with the current ontology
        int id;
        
        if (done.containsKey(entity))
            id = done.get(entity);
        
        else {
            indexNewEntity.setString(1, entity.getEntityType().getName());
            indexNewEntity.setString(2, entity.getIRI().toString());
            indexNewEntity.executeUpdate();
            
            try (ResultSet generatedKeys = indexNewEntity.getGeneratedKeys()) {
                generatedKeys.next();
                id = generatedKeys.getInt(1);
            }
            done.put(entity, id);
        }
        
        entityOntologyAssociation.setInt(1, id);
        entityOntologyAssociation.executeUpdate();
    }
    
    
    private static OWLEntity getEntity(String typeName, String iri) {
        EntityType<?> type = nameToType.get(typeName);
        // TODO If OWL-API changes the entity types, this will probably not make sense anymore.
        return factory.getOWLEntity(type, IRI.create(iri));
    }
    
    private PreparedStatement entityToIndex;
    private PreparedStatement indexToEntity;
    private PreparedStatement entityToOntologyID;
    
    private PreparedStatement setExtra;
    private PreparedStatement getExtra;
    private PreparedStatement getEntities;
    private PreparedStatement getAllEntities;
    
    private Hashtable<Integer, OWLEntity> idToEntity;
    private Hashtable<OWLEntity, Integer> entityToID;
    
    
    @SuppressWarnings("resource")
    @Override
    protected void extract(Set<OWLOntology> ontologies) throws SQLException {
        Statement statement = getConnection().createStatement();
        
        // Create the table that contains the IRI's of OWLEntities
        statement.execute("DROP TABLE IF EXISTS owl_objects");
        statement.execute(""
                + "CREATE TABLE owl_objects ("
                + "id INT PRIMARY KEY AUTO_INCREMENT,"
                + "type VARCHAR(32),"
                + "iri TEXT,"
                + "INDEX (type),"
                + "INDEX (iri(256)))");
        
        // Create table to associate each object with a certain loaded ontology
        statement.execute("DROP TABLE IF EXISTS object_ontology");
        statement.execute("CREATE TABLE object_ontology ("
                + "object_id INT,"
                + "ontology_id INT,"
                + "INDEX (object_id),"
                + "INDEX (ontology_id))");
        
        // Create the table for extra information
        statement.execute("DROP TABLE IF EXISTS extras");
        statement.execute("CREATE TABLE extras (tag VARCHAR(256), value TEXT, UNIQUE (tag))");
        
        statement.close();
        
        // Get the internal ID of each loaded ontology
        Hashtable<OWLOntology, Integer> ontologyInternalID = new Hashtable<>();
        PreparedStatement getOntologyIDStatement = getConnection()
                .prepareStatement("SELECT id FROM ontologies WHERE ontology_iri = ? AND version_iri = ?");
        for (OWLOntology ontology : ontologies) {
            OWLOntologyID ontologyID = ontology.getOntologyID();
            String ontologyIRIString = ontologyID.getOntologyIRI().toString();
            IRI versionIRI = ontologyID.getVersionIRI();
            String versionIRIString = versionIRI == null ? "" : versionIRI.toString();
            
            getOntologyIDStatement.setString(1, ontologyIRIString);
            getOntologyIDStatement.setString(2, versionIRIString);
            try (ResultSet resultSet = getOntologyIDStatement.executeQuery()) {
                if (!resultSet.next())
                    throw new SQLException("Unable to find " + ontologyID + " on the database");
                ontologyInternalID.put(ontology, resultSet.getInt(1));
            }
        }
        getOntologyIDStatement.close();
        
        // Now, let's find all the named entities of these ontologies and put them in the database
        // Start with owl:Thing
        PreparedStatement indexNewEntity = getConnection()
                .prepareStatement("INSERT INTO owl_objects (type, iri) VALUES (?, ?)", Statement.RETURN_GENERATED_KEYS);
        PreparedStatement entityOntologyAssociation = getConnection()
                .prepareStatement("INSERT INTO object_ontology (object_id, ontology_id) VALUES (?, ?)");
        
        Hashtable<OWLEntity, Integer> done = new Hashtable<>();
        
        for (OWLOntology ontology : ontologies) {
            Set<OWLEntity> entities = ontology.getSignature(true);
            entityOntologyAssociation.setInt(2, ontologyInternalID.get(ontology));
            for (OWLEntity entity : entities) {
                extractEntityID(done, entity, indexNewEntity, entityOntologyAssociation);
            }
            extractEntityID(done, factory.getOWLThing(), indexNewEntity, entityOntologyAssociation);
        }
        indexNewEntity.close();
    }
    
    
    @Override
    protected void prepare() throws SQLException {
        Connection connection = getConnection();
        
        idToEntity = new Hashtable<>();
        entityToID = new Hashtable<>();
        
        entityToIndex = connection.prepareStatement(""
                + "SELECT id "
                + "FROM owl_objects "
                + "WHERE type = ? AND iri = ?");
        indexToEntity = connection.prepareStatement("SELECT type, iri FROM owl_objects WHERE id = ?");
        
        entityToOntologyID = connection.prepareStatement(""
                + "SELECT ontology_iri, version_iri "
                + "FROM owl_objects "
                + "JOIN ontologies ON ontologies.object_id = owl_objects.id "
                + "WHERE owl_objects.id = ?");
        
        setExtra = connection.prepareStatement("REPLACE INTO extras (tag, value) VALUES (?, ?)");
        getExtra = connection.prepareStatement("SELECT value FROM extras WHERE tag = ?");
        
        getEntities = connection.prepareStatement("SELECT iri FROM owl_objects WHERE type = ?");
        getAllEntities = connection.prepareStatement("SELECT type, iri FROM owl_objects");
    }
    
    
    public HashSet<OWLEntity> getAllEntities() throws SQLException {
        HashSet<OWLEntity> result = new HashSet<>();
        try (ResultSet resultSet = getAllEntities.executeQuery()) {
            while (resultSet.next()) {
                EntityType<?> type = nameToType.get(resultSet.getString(1));
                result.add(factory.getOWLEntity(type, IRI.create(resultSet.getString(1))));
            }
        }
        return result;
    }
    
    
    public <U extends OWLEntity> HashSet<U> getAllEntities(EntityType<U> type) throws SQLException {
        HashSet<U> result = new HashSet<>();
        getEntities.setString(1, type.getName());
        try (ResultSet resultSet = getEntities.executeQuery()) {
            while (resultSet.next()) {
                result.add(factory.getOWLEntity(type, IRI.create(resultSet.getString(1))));
            }
        }
        return result;
    }
    
    
    public Set<OWLOntologyID> getDefiningOntologies(OWLEntity entity) throws SQLException {
        Set<OWLOntologyID> result = new HashSet<>();
        entityToOntologyID.setInt(1, getID(entity));
        try (ResultSet resultSet = entityToOntologyID.executeQuery()) {
            while (resultSet.next()) {
                String ontologyString = resultSet.getString(1);
                IRI ontologyIRI = IRI.create(ontologyString);
                
                String versionString = resultSet.getString(2);
                IRI versionIRI = versionString.equals("") ? null : IRI.create(versionString);
                
                result.add(new OWLOntologyID(ontologyIRI, versionIRI));
            }
        }
        return result;
    }
    
    
    public OWLEntity getEntity(int id) throws SQLException {
        if (idToEntity.containsKey(id))
            return idToEntity.get(id);
        
        OWLEntity result = null;
        
        indexToEntity.setInt(1, id);
        try (ResultSet results = indexToEntity.executeQuery()) {
            if (results.next()) {
                String typeName = results.getString(1);
                String iri = results.getString(2);
                result = getEntity(typeName, iri);
            }
        }
        
        if (result != null)
            idToEntity.put(id, result);
        
        return result;
    }
    
    
    public String getExtra(String key) throws SQLException {
        if (key.length() > 256)
            throw new IllegalArgumentException(String.format("Supplied key is longer than 256 characters"));
        
        getExtra.setString(1, key);
        
        try (ResultSet resultSet = getExtra.executeQuery()) {
            if (resultSet.next())
                return resultSet.getString(1);
        }
        
        return null;
    }
    
    
    public int getID(OWLEntity entity) throws SQLException {
        if (entityToID.containsKey(entity))
            return entityToID.get(entity);
        
        int result = -1;
        
        entityToIndex.setString(1, entity.getEntityType().getName());
        entityToIndex.setString(2, entity.getIRI().toString());
        
        try (ResultSet results = entityToIndex.executeQuery()) {
            if (results.next())
                result = results.getInt(1);
        }
        
        entityToID.put(entity, result);
        return result;
    }
    
    
    public void setExtra(String key, String value) throws SQLException {
        if (key.length() > 256)
            throw new IllegalArgumentException(String.format("Supplied key is longer than 256 characters"));
        
        setExtra.setString(1, key);
        setExtra.setString(2, value);
        setExtra.executeUpdate();
    }
}
