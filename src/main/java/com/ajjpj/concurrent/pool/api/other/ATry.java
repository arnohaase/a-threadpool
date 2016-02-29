package com.ajjpj.concurrent.pool.api.other;

import com.ajjpj.afoundation.function.AFunction1;
import com.ajjpj.afoundation.function.APartialFunction;
import com.ajjpj.afoundation.function.AStatement1;


public abstract class ATry<T> {

    public static <T> ATry<T> success (T o) {
        return null; //TODO
    }

    public static <T> ATry<T> failure (Throwable th) {
        return null; //TODO
    }

    public abstract <X extends Throwable> void foreach (AStatement1<T,X> f);

    public abstract ATry<Throwable> inverse();

    public abstract <S, E extends Throwable> ATry<S> map (AFunction1<T, S, E> f) throws E;

    public abstract boolean isSuccess();

    public boolean isFailure () {
        return ! isSuccess();
    }

    public abstract T getValue ();

    public abstract <E extends Throwable> ATry<T> recover (APartialFunction<Throwable, T, E> f) throws E;

    //TODO
}
