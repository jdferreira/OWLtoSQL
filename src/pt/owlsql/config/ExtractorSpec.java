package pt.owlsql.config;

import java.util.Hashtable;

import pt.owlsql.Extractor;

import com.google.gson.JsonElement;

public final class ExtractorSpec<E extends Extractor> {
    
    private final Class<E> cls;
    private final Hashtable<String, JsonElement> parameters;
    
    
    public ExtractorSpec(Class<E> cls) {
        this.cls = cls;
        this.parameters = new Hashtable<>();
    }
    
    
    public ExtractorSpec(Class<E> cls, Hashtable<String, JsonElement> parameters) {
        this.cls = cls;
        this.parameters = parameters;
    }
    
    
    public Class<E> getExtractorClass() {
        return cls;
    }
    
    
    public Hashtable<String, JsonElement> getParameters() {
        return parameters;
    }
}