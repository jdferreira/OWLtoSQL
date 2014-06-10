package pt.owlsql;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Modifier;
import java.net.URI;
import java.util.ArrayList;
import java.util.Hashtable;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;


final class ConfigReader {
    
    // Start by defining some constants
    
    // Default configuration file
    static final String CONFIG_FILE = "owlsql-config.xml";
    
    // <elements>
    private static final String OWLSQL = "owlsql";
    private static final String VARIABLE = "variable";
    private static final String ONTOLOGY = "ontology";
    private static final String MYSQL = "mysql";
    private static final String EXTRACTOR = "extractor";
    private static final String EXECUTABLE = "executable";
    private static final String PARAMETER = "parameter";
    private static final String ITEM = "item";
    
    // "attribute_names"
    private static final String NAME = "name";
    private static final String URL = "url";
    private static final String HOST = "host";
    private static final String DATABASE = "database";
    private static final String USERNAME = "username";
    private static final String PASSWORD = "password";
    private static final String CLASS = "class";
    private static final String LIST = "list";
    private static final String VALUE = "value";
    
    
    private static final Hashtable<String, String> variables = new Hashtable<>();
    private static final ArrayList<URI> ontologies = new ArrayList<>();
    private static final ArrayList<Class<? extends Extractor>> executableClasses = new ArrayList<>();
    private static final Hashtable<Class<? extends Extractor>, Hashtable<String, Object>> moreOptions = new Hashtable<>();
    
    private static XMLEventReader eventReader;
    
    private static String host;
    private static String database;
    private static String username;
    private static String password;
    
    
    private static void ensureEndElement(String elementName) throws XMLStreamException, ConfigException {
        XMLEvent event = eventReader.peek();
        if (!event.isEndElement())
            throw new ConfigException(elementName + " does not expect inner elements");
    }
    
    
    private static String getAttribute(StartElement startElement, String name, boolean mandatory)
            throws ResolutionException, InvalidStringException, ConfigException {
        Attribute attribute = startElement.getAttributeByName(new QName(name));
        if (attribute == null) {
            if (mandatory)
                throw new ConfigException("Element <"
                        + startElement.getName().getLocalPart()
                        + "> expects an attribute \""
                        + name
                        + "\".");
            else
                return null;
        }
        return resolve(attribute.getValue());
    }
    
    
    private static ArrayList<String> getParameterListValues() throws XMLStreamException, ConfigException {
        ArrayList<String> result = new ArrayList<>();
        while (eventReader.hasNext()) {
            XMLEvent event = eventReader.nextEvent();
            if (event.isStartElement()) {
                StartElement otherStartElement = event.asStartElement();
                String elementName = otherStartElement.getName().getLocalPart();
                if (elementName.equals(ITEM)) {
                    String value;
                    try {
                        value = getAttribute(otherStartElement, VALUE, true);
                    }
                    catch (ResolutionException | InvalidStringException e) {
                        System.err.println("Unable to get a value from one of the <item> elements: " + e.getMessage());
                        continue;
                    }
                    result.add(value);
                }
                else {
                    System.err.println("A <parameter> element has an unexpected <" + elementName + "> child");
                    break;
                }
            }
            if (event.isEndElement() && event.asEndElement().getName().getLocalPart().equals(PARAMETER))
                break;
        }
        return result;
    }
    
    
    private static String getParameterValue(StartElement startElement) throws ResolutionException,
            InvalidStringException, ConfigException {
        return getAttribute(startElement, VALUE, true);
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
    
    
    private static void parseExecutable(StartElement startElement) throws XMLStreamException, ConfigException {
        String className;
        try {
            className = getAttribute(startElement, CLASS, true);
        }
        catch (ResolutionException | InvalidStringException e) {
            System.err.println("Unable to get the <executable> class attribute: " + e.getMessage());
            return;
        }
        
        Class<? extends Extractor> cls;
        try {
            cls = validateClass(className, Extractor.class);
        }
        catch (ClassNotFoundException | InitException e) {
            e.printStackTrace();
            return;
        }
        if (OWLExtractor.class.isAssignableFrom(cls)) {
            System.err.println("Cannot use <executable> with an implementation of the " + OWLExtractor.class.getName());
            return;
        }
        
        executableClasses.add(cls);
        
        // Now let's find more options
        while (eventReader.hasNext()) {
            XMLEvent event = eventReader.nextEvent();
            if (event.isStartElement()) {
                StartElement otherStartElement = event.asStartElement();
                String elementName = otherStartElement.getName().getLocalPart();
                if (elementName.equals(PARAMETER))
                    parseExtractorParameter(cls, otherStartElement);
                else {
                    System.err.println("A <executable> element has an unexpected <" + elementName + "> child");
                    return;
                }
            }
            else if (event.isEndElement())
                return;
        }
    }
    
    
    private static void parseExtractor(StartElement startElement) throws XMLStreamException, ConfigException {
        String className;
        try {
            className = getAttribute(startElement, CLASS, true);
        }
        catch (ResolutionException | InvalidStringException e) {
            System.err.println("Unable to get the <extractor> class attribute: " + e.getMessage());
            return;
        }
        
        Class<? extends OWLExtractor> cls;
        try {
            cls = validateClass(className, OWLExtractor.class);
        }
        catch (ClassNotFoundException | InitException e) {
            e.printStackTrace();
            return;
        }
        executableClasses.add(cls);
        
        // Now let's find more options
        while (eventReader.hasNext()) {
            XMLEvent event = eventReader.nextEvent();
            if (event.isStartElement()) {
                StartElement otherStartElement = event.asStartElement();
                String elementName = otherStartElement.getName().getLocalPart();
                if (elementName.equals(PARAMETER))
                    parseExtractorParameter(cls, otherStartElement);
                else {
                    System.err.println("A <extractor> element has an unexpected <" + elementName + "> child");
                    return;
                }
            }
            else if (event.isEndElement())
                return;
        }
    }
    
    
    private static void parseExtractorParameter(Class<? extends Extractor> cls, StartElement startElement)
            throws XMLStreamException, ConfigException {
        String name;
        String list;
        try {
            name = getAttribute(startElement, NAME, true);
            list = getAttribute(startElement, LIST, false);
        }
        catch (ResolutionException | InvalidStringException e) {
            // "list" attribute will never return this exception!
            System.err.println("Unable to get the Parameter name attribute: " + e.getMessage());
            return;
        }
        
        Object value;
        if (Boolean.parseBoolean(list))
            value = getParameterListValues();
        else {
            try {
                value = getParameterValue(startElement);
            }
            catch (ResolutionException | InvalidStringException e) {
                System.err.println("Unable to get a value from one of the <parameter> elements: " + e.getMessage());
                return;
            }
        }
        
        putMoreOptions(cls, name, value);
    }
    
    
    private static void parseOntology(StartElement startElement) throws ConfigException {
        String url;
        try {
            url = getAttribute(startElement, URL, true);
        }
        catch (ResolutionException | InvalidStringException e) {
            System.err.println("Unable to get the attribute: " + e.getMessage());
            return;
        }
        
        ontologies.add(URI.create(url));
    }
    
    
    private static void parseSQL(StartElement startElement) throws ConfigException {
        try {
            host = getAttribute(startElement, HOST, false);
            if (host == null)
                host = "localhost";
            database = getAttribute(startElement, DATABASE, true);
            username = getAttribute(startElement, USERNAME, true);
            password = getAttribute(startElement, PASSWORD, true);
        }
        catch (ResolutionException | InvalidStringException e) {
            System.err.println("Unable to get the MySQL parameters: " + e.getMessage());
        }
    }
    
    
    private static void parseVariable(StartElement startElement) throws ConfigException {
        // Get the "name" and "value" attributes
        String name;
        try {
            name = getAttribute(startElement, NAME, true);
        }
        catch (ResolutionException | InvalidStringException e) {
            System.err.println("Unable to get the attribute: " + e.getMessage());
            return;
        }
        
        // Validate this name
        if (!isValidName(name)) {
            System.err.println("Variable names can only contain characters from [0-9a-zA-Z_.-]");
            return;
        }
        
        String value;
        try {
            value = resolve(getAttribute(startElement, VALUE, true));
        }
        catch (ResolutionException e) {
            String inner = e.getMessage();
            System.err.println("Variable \"" + name + "\" contains an undefined reference to ${" + inner + "}");
            return;
        }
        catch (InvalidStringException e) {
            System.err.println("Variable \"" + name + "\" contains an invalid value");
            if (e.getMessage() != null)
                System.err.println(e.getMessage());
            return;
        }
        variables.put(name, value);
    }
    
    
    private static void putMoreOptions(Class<? extends Extractor> cls, String name, Object value) {
        Hashtable<String, Object> options = moreOptions.get(cls);
        if (options == null)
            moreOptions.put(cls, options = new Hashtable<>());
        options.put(name, value);
    }
    
    
    private static String resolve(String string) throws ResolutionException, InvalidStringException {
        int index = 0;
        StringBuilder sb = new StringBuilder();
        while (true) {
            // Find the first dollar-sign
            int dollarIndex = string.indexOf('$', index);
            
            // No dollar-sign? Append whatever is left to the string builder and return
            if (dollarIndex == -1) {
                sb.append(string.substring(index));
                return sb.toString();
            }
            
            // Is this followed by another dollar sign? If so, consume one of them and continue
            if (dollarIndex + 1 < string.length() && string.charAt(dollarIndex + 1) == '$') {
                sb.append(string.substring(index, dollarIndex + 1));
                index = dollarIndex + 2;
                continue;
            }
            
            // So, this dollar-sign must be followed by { and then valid characters and then }
            if (string.charAt(dollarIndex + 1) != '{')
                throw new InvalidStringException(string);
            int endName = string.indexOf('}', dollarIndex + 2);
            if (endName == -1)
                throw new InvalidStringException("'${' not followed by '}'");
            if (endName == dollarIndex + 2)
                throw new InvalidStringException("Empty variable name '${}'");
            
            String possibleName = string.substring(dollarIndex + 2, endName);
            if (!isValidName(possibleName))
                throw new InvalidStringException("Invalid variable name");
            if (!variables.containsKey(possibleName))
                throw new ResolutionException(possibleName);
            
            sb.append(string.substring(index, dollarIndex));
            sb.append(variables.get(possibleName));
            index = endName + 1;
        }
    }
    
    
    @SuppressWarnings("unchecked")
    private static Class<? extends OWLExtractor> validateClass(String className, Class<?> superclass)
            throws InitException, ClassNotFoundException {
        // Get the class from the name
        Class<?> cls;
        cls = Class.forName(className);
        
        // There are a few restrictions on the classes that are valid Executables implementations
        if (cls.isAnonymousClass())
            throw new InitException(className + " cannot be an anonymous class.");
        if (cls.isInterface())
            throw new InitException(className + " cannot be an interface.");
        if (cls.isLocalClass())
            throw new InitException(className + " cannot be a local class.");
        if (cls.isMemberClass())
            throw new InitException(className + " cannot be a member class.");
        if (cls.isSynthetic())
            throw new InitException(className + " cannot be a synthetic class.");
        if (Modifier.isAbstract(cls.getModifiers()))
            throw new InitException(className + " cannot be an abstract class.");
        
        if (!superclass.isAssignableFrom(cls))
            throw new InitException(className + " does not extend " + superclass.getName());
        
        if (!Modifier.isFinal(cls.getModifiers()))
            throw new InitException(className + " must be final class.");
        
        // No duplicate extractors allowed
        if (executableClasses.contains(cls))
            throw new InitException("Duplicate OWLExtractor class detected: " + className);
        
        return (Class<? extends OWLExtractor>) cls;
    }
    
    
    public static String getDatabase() {
        return database;
    }
    
    
    public static ArrayList<Class<? extends Extractor>> getExtractorClasses() {
        return new ArrayList<>(executableClasses);
    }
    
    
    public static String getExtractorOption(Class<? extends Extractor> cls, String name) {
        if (!moreOptions.containsKey(cls))
            return null;
        return (String) moreOptions.get(cls).get(name);
    }
    
    
    public static ArrayList<String> getExtractorOptionList(Class<? extends Extractor> cls, String name) {
        if (!moreOptions.containsKey(cls))
            return null;
        return new ArrayList<>((ArrayList<String>) moreOptions.get(cls).get(name));
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
    
    
    @SuppressWarnings("resource")
    public static void initialize(String configFilename) throws ConfigException {
        XMLInputFactory inputFactory = XMLInputFactory.newInstance();
        InputStream in;
        try {
            in = new FileInputStream(configFilename);
        }
        catch (FileNotFoundException e) {
            throw new ConfigException("Cannot find file '" + configFilename + "'");
        }
        
        try {
            eventReader = inputFactory.createXMLEventReader(in);
            try {
                // read the XML document
                boolean inside = false;
                while (eventReader.hasNext()) {
                    XMLEvent event = eventReader.nextEvent();
                    if (event.isStartElement()) {
                        StartElement startElement = event.asStartElement();
                        String elementName = startElement.getName().getLocalPart();
                        if (!inside) {
                            if (!elementName.equals(OWLSQL))
                                throw new ConfigException("Expecting a <"
                                        + OWLSQL
                                        + "> element as root of the config file.");
                            inside = true;
                            continue;
                        }
                        
                        if (elementName.equals(VARIABLE)) {
                            parseVariable(startElement);
                            ensureEndElement(elementName);
                        }
                        else if (elementName.equals(ONTOLOGY)) {
                            parseOntology(startElement);
                            ensureEndElement(elementName);
                        }
                        else if (elementName.equals(MYSQL)) {
                            parseSQL(startElement);
                            ensureEndElement(elementName);
                        }
                        else if (elementName.equals(EXTRACTOR)) {
                            parseExtractor(startElement);
                        }
                        else if (elementName.equals(EXECUTABLE)) {
                            parseExecutable(startElement);
                        }
                    }
                }
            }
            finally {
                try {
                    in.close();
                }
                catch (IOException e) {}
            }
        }
        catch (XMLStreamException e) {
            throw new ConfigException(e);
        }
    }
}
