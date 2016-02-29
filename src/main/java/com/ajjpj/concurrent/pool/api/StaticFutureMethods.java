package com.ajjpj.concurrent.pool.api;

import com.ajjpj.afoundation.function.AFunction0;


class StaticFutureMethods {
    public static <T, E extends Throwable> AFuture<T> submit (AThreadPool tp, AFunction0<T, E> f) {
        final AFutureImpl<T> result = new AFutureImpl<> (tp);
        tp.submit (() -> {
            try {
                result.completeAsSuccess (f.apply ());
            }
            catch (Throwable th) {
                result.completeAsFailure (th);
                //TODO distinguish between safe and unsafe throwables
            }
        });
        return result;
    }
}
