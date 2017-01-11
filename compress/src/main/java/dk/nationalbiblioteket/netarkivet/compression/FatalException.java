package dk.nationalbiblioteket.netarkivet.compression;

/**
 * Exceptions which should never occur like mismatched checksums
 */
public class FatalException extends Exception {
    public FatalException(Throwable cause) {
        super(cause);
    }

    public FatalException(String message, Throwable cause) {
        super(message, cause);
    }

    public FatalException(String message) {
        super(message);
    }

}
