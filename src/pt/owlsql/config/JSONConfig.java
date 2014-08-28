package pt.owlsql.config;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Modifier;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map.Entry;

import pt.json.JSONException;
import pt.owlsql.Cacher;
import pt.owlsql.Extractor;
import pt.owlsql.OWLExtractor;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;


public final class JSONConfig {
    
    private static class Resolution {
        
        private final ArrayList<String> parts;
        private final ArrayList<String> variableNames;
        
        private HashSet<String> dependencies;
        
        
        private Resolution(ArrayList<String> parts, ArrayList<String> variableNames) {
            this.parts = parts;
            this.variableNames = variableNames;
        }
        
        
        public HashSet<String> getDependencies() {
            if (dependencies != null)
                return dependencies;
            
            dependencies = new HashSet<>();
            for (String variableName : variableNames) {
                if (variableName != null)
                    dependencies.add(variableName);
            }
            return dependencies;
        }
        
        
        public String resolve() {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < parts.size(); i++) {
                if (parts.get(i) != null)
                    sb.append(parts.get(i));
                else
                    sb.append(variables.get(variableNames.get(i)));
            }
            return sb.toString();
        }
    }
    
    
    public static final String CONFIG_FILE = "owlsql-config.json";
    
    private static JsonObject doc;
    
    private static String host;
    private static String database;
    private static String password;
    private static String username;
    
    private static final ArrayList<ExtractorSpec<?>> extractorSpecs = new ArrayList<>();
    
    private static final ArrayList<URI> ontologies = new ArrayList<>();
    private static final Hashtable<String, String> variables = new Hashtable<>();
    
    
    private static void detectCycles() throws JSONException {
        // We now need to make sure that no cycles exist and then replace the value of each variable with its resolved
        // value
        Hashtable<String, HashSet<String>> dependsOn = new Hashtable<>();
        Hashtable<String, HashSet<String>> isDependencyOf = new Hashtable<>();
        
        // Initialize the isDependencyOf map.
        for (String varName : variables.keySet()) {
            isDependencyOf.put(varName, new HashSet<String>());
        }
        
        Hashtable<String, Resolution> resolutions = new Hashtable<>();
        for (Entry<String, String> entry : variables.entrySet()) {
            Resolution resolution;
            String varName = entry.getKey();
            
            try {
                resolution = getResolution(entry.getValue());
            }
            catch (JSONException e) {
                throw e.withPrefix("variables", varName);
            }
            if (resolution.getDependencies().contains(varName))
                throw new JSONException("Variable is recursive", "variables", varName);
            
            resolutions.put(varName, resolution);
            
            dependsOn.put(varName, resolution.getDependencies());
            for (String dependency : resolution.getDependencies()) {
                isDependencyOf.get(dependency).add(varName);
            }
        }
        
        // While there are variables with no dependencies, remove them
        while (dependsOn.size() > 0) {
            String noDependencies = null;
            for (Entry<String, HashSet<String>> entry : dependsOn.entrySet()) {
                if (entry.getValue().size() == 0) {
                    noDependencies = entry.getKey();
                    break;
                }
            }
            if (noDependencies != null) {
                // This variable does not have any dependencies (or all of them can be resolved)
                // so it can also be resolved. Thus, it is the next one to be resolved
                variables.put(noDependencies, resolutions.get(noDependencies).resolve());
                
                // This key does not have any dependency. Remove it from our sight
                dependsOn.remove(noDependencies);
                for (String dependent : isDependencyOf.get(noDependencies)) {
                    dependsOn.get(dependent).remove(noDependencies);
                }
                continue;
            }
            
            // If we're here, then there is at least one cycle.
            // Find one and report it
            
            List<String> cycle = new ArrayList<>();
            String varName = dependsOn.keySet().iterator().next();
            int index = -1;
            while ((index = cycle.indexOf(varName)) == -1) {
                cycle.add(varName);
                varName = dependsOn.get(varName).iterator().next();
            }
            cycle = cycle.subList(index, cycle.size());
            StringBuilder sb = new StringBuilder();
            for (String string : cycle) {
                sb.append(" > ").append(string);
            }
            throw new JSONException("Cyclic variable dependency detected: " + sb.substring(3), "variables");
        }
    }
    
    
    private static Resolution getResolution(String string) throws JSONException {
        int index = 0;
        
        ArrayList<String> parts = new ArrayList<>();
        ArrayList<String> variableNames = new ArrayList<>();
        
        while (true) {
            // Find the first dollar-sign
            int dollarIndex = string.indexOf('$', index);
            
            // No dollar-sign? Append whatever is left as a part and return the resolution object
            if (dollarIndex == -1) {
                parts.add(string.substring(index));
                variableNames.add(null);
                return new Resolution(parts, variableNames);
            }
            
            // Is this followed by another dollar sign? If so, consume one of them and continue
            if (dollarIndex + 1 < string.length() && string.charAt(dollarIndex + 1) == '$') {
                parts.add(string.substring(index, dollarIndex + 1));
                variableNames.add(null);
                index = dollarIndex + 2;
                continue;
            }
            
            // We need to take care of the characters up to this point
            parts.add(string.substring(index, dollarIndex));
            variableNames.add(null);
            
            // So, this dollar-sign must be followed by { and then a valid variable name and then }
            if (string.charAt(dollarIndex + 1) != '{')
                throw new JSONException("Expecting '{' after the dollar sign");
            int endName = string.indexOf('}', dollarIndex + 2);
            if (endName == -1)
                throw new JSONException("'${' not followed by '}'");
            if (endName == dollarIndex + 2)
                throw new JSONException("Empty variable name '${}'");
            
            String possibleName = string.substring(dollarIndex + 2, endName);
            if (!isValidName(possibleName))
                throw new JSONException("Invalid variable name '" + possibleName + "'");
            
            parts.add(null);
            variableNames.add(possibleName);
            index = endName + 1;
        }
    }
    
    
    private static boolean isValidName(String name) {
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            if (('0' <= c || c <= '9')
                    || ('a' <= c && c <= 'z')
                    || ('A' <= c && c <= 'Z')
                    || c == '_'
                    || c == '-'
                    || c == '.')
                continue;
            return false;
        }
        return true;
    }
    
    
    private static Class<? extends Extractor> getExtractorClass(String classname) throws JSONException {
        Class<?> cls;
        try {
            cls = Class.forName(classname);
        }
        catch (ClassNotFoundException e) {
            throw new JSONException("class not found");
        }
        validateClass(cls);
        
        return cls.asSubclass(Extractor.class);
    }
    
    
    private static ExtractorSpec<?> processExtractor(JsonObject object) throws JSONException {
        if (!object.has("class"))
            throw new JSONException("must have a \"class\" element");
        
        JsonElement classElement = object.remove("class");
        if (!classElement.isJsonPrimitive() || !classElement.getAsJsonPrimitive().isString())
            throw new JSONException("must be a string", "class");
        
        Class<? extends Extractor> extractorClass;
        try {
            extractorClass = getExtractorClass(classElement.getAsString());
        }
        catch (JSONException e) {
            throw e.withPrefix("class");
        }
        
        Hashtable<String, JsonElement> parameters = new Hashtable<>();
        for (Entry<String, JsonElement> entry : object.entrySet()) {
            parameters.put(entry.getKey(), entry.getValue());
        }
        return new ExtractorSpec<>(extractorClass, parameters);
    }
    
    
    private static ExtractorSpec<?> processExtractor(String classname) throws JSONException {
        Class<? extends Extractor> extractorClass = getExtractorClass(classname);
        return new ExtractorSpec<>(extractorClass);
    }
    
    
    private static void processExtractors() throws JSONException {
        JsonElement element = doc.get("extractors");
        if (element == null)
            return;
        else if (!element.isJsonArray())
            throw new JSONException("must be a JSON array", "extractors");
        JsonArray array = element.getAsJsonArray();
        
        for (int i = 0; i < array.size(); i++) {
            JsonElement inner = array.get(i);
            ExtractorSpec<?> spec;
            if (inner.isJsonPrimitive() && inner.getAsJsonPrimitive().isString()) {
                try {
                    spec = processExtractor(resolve(inner.getAsString()));
                }
                catch (JSONException e) {
                    throw e.withPrefix("extractors", "[" + i + "]");
                }
            }
            else if (inner.isJsonObject()) {
                try {
                    spec = processExtractor(inner.getAsJsonObject());
                }
                catch (JSONException e) {
                    throw e.withPrefix("extractors", "[" + i + "]");
                }
            }
            else
                throw new JSONException("must be either a string or a JSON object", "extractors", "[" + i + "]");
            
            extractorSpecs.add(spec);
        }
        
    }
    
    
    private static void processOntologies() throws JSONException {
        JsonElement element = doc.get("ontologies");
        if (element == null)
            return;
        else if (!element.isJsonArray())
            throw new JSONException("must be a JSON array", "ontologies");
        JsonArray array = element.getAsJsonArray();
        
        for (int i = 0; i < array.size(); i++) {
            JsonElement inner = array.get(i);
            if (!inner.isJsonPrimitive() || !inner.getAsJsonPrimitive().isString())
                throw new JSONException("must be a JSON array", "ontologies", "[" + i + "]");
            
            URI url;
            try {
                url = URI.create(resolve(inner.getAsString()));
            }
            catch (IllegalArgumentException e) {
                throw new JSONException("Malformed URL", e, "ontologies", "[" + i + "]");
            }
            catch (JSONException e) {
                throw e.withPrefix("ontologies", "[" + i + "]");
            }
            
            ontologies.add(url);
        }
    }
    
    
    private static void processSQLParams() throws JSONException {
        JsonElement element = doc.get("mysql");
        if (element == null)
            throw new JSONException("object \"mysql\" is mandatory");
        else if (!element.isJsonObject())
            throw new JSONException("must be a JSON object", "mysql");
        JsonObject object = element.getAsJsonObject();
        
        JsonElement databaseElement = object.get("database");
        if (databaseElement == null)
            throw new JSONException("must have a \"database\" value", "mysql");
        else if (!databaseElement.isJsonPrimitive() || !databaseElement.getAsJsonPrimitive().isString())
            throw new JSONException("must be a string", "mysql", "database");
        else {
            try {
                database = resolve(databaseElement.getAsString());
            }
            catch (JSONException e) {
                throw e.withPrefix("mysql", "database");
            }
        }
        
        JsonElement usernameElement = object.get("username");
        if (usernameElement == null)
            throw new JSONException("must have a \"username\" value", "mysql");
        else if (!usernameElement.isJsonPrimitive() || !usernameElement.getAsJsonPrimitive().isString())
            throw new JSONException("must be a string", "mysql", "username");
        else {
            try {
                username = resolve(usernameElement.getAsString());
            }
            catch (JSONException e) {
                throw e.withPrefix("mysql", "username");
            }
        }
        
        JsonElement passwordElement = object.get("password");
        if (passwordElement == null)
            throw new JSONException("must have a \"password\" value", "mysql");
        else if (!passwordElement.isJsonPrimitive() || !passwordElement.getAsJsonPrimitive().isString())
            throw new JSONException("must be a string", "mysql", "password");
        else {
            try {
                password = resolve(passwordElement.getAsString());
            }
            catch (JSONException e) {
                throw e.withPrefix("mysql", "password");
            }
        }
        
        JsonElement hostElement = object.get("host");
        if (hostElement == null)
            host = "localhost";
        else if (!hostElement.isJsonPrimitive() || !hostElement.getAsJsonPrimitive().isString())
            throw new JSONException("must be a string", "mysql", "host");
        else {
            try {
                host = resolve(hostElement.getAsString());
            }
            catch (JSONException e) {
                throw e.withPrefix("mysql", "host");
            }
        }
    }
    
    
    private static void processVariables() throws JSONException {
        JsonElement element = doc.get("variables");
        if (element == null)
            return;
        else if (!element.isJsonObject())
            throw new JSONException("must be a JSON object", "variables");
        JsonObject object = element.getAsJsonObject();
        
        for (Entry<String, JsonElement> entry : object.entrySet()) {
            JsonElement value = entry.getValue();
            if (!value.isJsonPrimitive() || !value.getAsJsonPrimitive().isString())
                throw new JSONException("must be a string", "variables", entry.getKey());
            variables.put(entry.getKey(), value.getAsString());
        }
        
        detectCycles();
    }
    
    
    private static void readDocument() throws JSONException {
        processVariables();
        processSQLParams();
        processOntologies();
        processExtractors();
    }
    
    
    private static void validateClass(Class<?> cls) throws JSONException {
        // There are a few restrictions on the classes that are valid Executables implementations
        if (cls.equals(Extractor.class)
                || cls.equals(OWLExtractor.class)
                || cls.equals(Cacher.class)
                || (!OWLExtractor.class.isAssignableFrom(cls) && Cacher.class.isAssignableFrom(cls)))
            throw new JSONException("must extend either "
                    + OWLExtractor.class.getName()
                    + " or "
                    + Cacher.class.getName());
        
        if (cls.isAnonymousClass())
            throw new JSONException(cls.getName() + " must not be an anonymous class");
        if (cls.isInterface())
            throw new JSONException(cls.getName() + " must not be an interface");
        if (cls.isLocalClass())
            throw new JSONException(cls.getName() + " must not be a local class");
        if (cls.isMemberClass())
            throw new JSONException(cls.getName() + " must not be a member class");
        if (cls.isSynthetic())
            throw new JSONException(cls.getName() + " must not be a synthetic class");
        
        if (Modifier.isAbstract(cls.getModifiers()))
            throw new JSONException(cls.getName() + " must not be an abstract class");
        if (!Modifier.isFinal(cls.getModifiers()))
            throw new JSONException(cls.getName() + " must be a final class");
    }
    
    
    public static String getDatabase() {
        return database;
    }
    
    
    public static ArrayList<ExtractorSpec<?>> getExtractorSpecs() {
        return extractorSpecs;
    }
    
    
    public static String getHost() {
        return host;
    }
    
    
    public static ArrayList<URI> getOntologiesURI() {
        return new ArrayList<>(ontologies);
    }
    
    
    public static String getPassword() {
        return password;
    }
    
    
    public static String getUsername() {
        return username;
    }
    
    
    public static void main(String[] args) throws IOException, JSONException {
        JSONConfig.read(CONFIG_FILE);
    }
    
    
    public static void read(String filename) throws IOException, JSONException {
        try (FileInputStream stream = new FileInputStream(filename)) {
            JsonParser parser = new JsonParser();
            try (InputStreamReader reader = new InputStreamReader(stream)) {
                doc = parser.parse(reader).getAsJsonObject();
            }
            
            readDocument();
        }
    }
    
    
    public static String resolve(String string) throws JSONException {
        return getResolution(string).resolve();
    }
    
    
    private JSONConfig() {
        throw new RuntimeException("Cannot instantiate this class");
    }
}
