package com.ajjpj.concurrent.pool.api.other;


import com.ajjpj.afoundation.function.AStatement1;


/**
 * Represents a partial function that returns nothing, having {@code void} as its return value.
 */
public interface APartialStatement<P,T extends Throwable> extends AStatement1<P,T> {
    boolean isDefinedAt (P param);
}
