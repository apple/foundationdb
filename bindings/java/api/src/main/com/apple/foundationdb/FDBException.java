package com.apple.foundationdb;

import com.apple.foundationdb.async.CloneableException;

/**
 * An Error from FoundationDB.  Each {@code FDBException} sets
 *  the {@code message} of the underlying Java {@link Exception}. FDB exceptions expose
 *  a number of functions including, for example, {@link #isRetryable()} that
 *  evaluate predicates on the internal FDB error. Most clients should use those methods
 *  in order to implement special handling for certain errors if their application
 *  requires it.
 *
 * <p>
 * Errors in FDB should generally be retried if they match the {@link #isRetryable()}
 *  predicate. In addition, as with any distributed system, certain classes of errors
 *  may fail in such a way that it is unclear whether the transaction succeeded (they
 *  {@link #isMaybeCommitted() may be committed} or not). To handle these cases, clients
 *  are generally advised to make their database operations idempotent and to place
 *  their operations within retry loops. The FDB Java API provides some default retry loops
 *  within the {@link Database} interface. See the discussion within the documentation of
 *  {@link Database#runAsync(Function) Database.runAsync()} for more details.
 *
 * @see Transaction#onError(Throwable) Transaction.onError()
 * @see Database#runAsync(Function) Database.runAsync()
 */
public abstract class FDBException extends RuntimeException implements CloneableException {

    protected final int code;

    /**
     * Constructor to be used by subclasses
     *
     * @param message error message of this exception
     * @param code internal FDB error code of this exception
     */
    public FDBException(String message, int code) {
        super(message);
        this.code = code;
    }

    /**
     * Gets the code for this error. A list of common errors codes
     *  are published <a href="/foundationdb/api-error-codes.html">elsewhere within
     *  our documentation</a>.
     *
     * @return the internal FDB error code
     */
    public int getCode() {
        return code;
    }

    /**
     * Determine if this {@code FDBException} represents a success code from the native layer.
     *
     * @return {@code true} if this error represents success, {@code false} otherwise
     */
    public abstract boolean isSuccess();

    /**
     * Returns {@code true} if the error indicates the operations in the transactions should be retried because of transient error.
     *
     * @return {@code true} if this {@code FDBException} is {@code retryable}
     */
    public abstract boolean isRetryable();

    /**
     * Returns {@code true} if the error indicates the transaction may have succeeded, though not in a way the system can verify.
     *
     * @return {@code true} if this {@code FDBException} is {@code maybe_committed}
     */
    public abstract boolean isMaybeCommitted();

    /**
     * Returns {@code true} if the error indicates the transaction has not committed, though in a way that can be retried.
     *
     * @return {@code true} if this {@code FDBException} is {@code retryable_not_committed}
     */
    public abstract boolean isRetryableNotCommitted();
}
