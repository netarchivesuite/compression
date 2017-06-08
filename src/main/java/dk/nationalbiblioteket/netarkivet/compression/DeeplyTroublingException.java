package dk.nationalbiblioteket.netarkivet.compression;

/**
 * Exceptions which should never occur like mismatched checksums
 */
public class DeeplyTroublingException extends Exception {
    public DeeplyTroublingException(Throwable cause) {
        super(cause);
    }

    public DeeplyTroublingException(String message, Throwable cause) {
        super(message, cause);
    }

    public DeeplyTroublingException(String message) {
        super(message);
    }

}
