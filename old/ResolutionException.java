package pt.owlsql;

public class ResolutionException extends Exception {
    
    private static final long serialVersionUID = 1495632258465794811L;
    
    
    public ResolutionException(String innerVariableName) {
        super(innerVariableName);
    }
}
