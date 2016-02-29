package com.ajjpj.concurrent.pool.api;

import com.ajjpj.afoundation.collection.immutable.AOption;
import com.ajjpj.afoundation.collection.tuples.ATuple2;
import com.ajjpj.afoundation.function.*;
import com.ajjpj.concurrent.pool.api.other.APartialStatement;
import com.ajjpj.concurrent.pool.api.other.ATry;

import java.util.List;
import java.util.concurrent.Callable;


//TODO this AFuture's thread pool as default thread pool

public interface AFuture<T> {
//    void ready (long amount, TimeUnit timeUnit) throws TimeoutException;
//    T await (long amount, TimeUnit timeUnit) throws TimeoutException; //TODO subclass without stacktrace

    //TODO onSuccess, onFailure with APartialStatement
    default AFuture<T> onSuccess  (AThreadPool tp, AStatement1<T, ?> handler) {
        return onComplete (tp, t -> t.foreach (handler));
    }
    default AFuture<T> onFailure  (AThreadPool tp, AStatement1<Throwable, ?> handler) {
        return onComplete (tp, t -> t.inverse ().foreach (handler));
    }

    AFuture<T> onComplete (AThreadPool tp, AStatement1<ATry<T>, ?> handler);

    boolean isComplete();
    AOption<ATry<T>> optValue();

    AFuture<Throwable> inverse();

//    <S> AFuture<S> transform (AThreadPool tp, AFunction1<T, S, ?> s, AFunction1<Throwable, Throwable, ?> t);

    <S> AFuture<S> map (AThreadPool tp, AFunction1<T, S, ?> f);
    <S> AFuture<S> flatMap (AThreadPool tp, AFunction1<T, AFuture<S>, ?> f);

    AFuture<T> filter (AThreadPool tp, APredicate<T, ?> f);

    <S> AFuture<S> collect (AThreadPool tp, APartialFunction<T, S, ?> f);

    AFuture<T> recover (AThreadPool tp, APartialFunction<Throwable, T, ?> f);

    <S> AFuture<ATuple2<T,S>> zip (AFuture<S> that);
    AFuture<T> fallbackTo (AFuture<T> that);

    AFuture<T> andThen (AThreadPool tp, APartialStatement<ATry<T>,?> f);


    static <T> AFuture<T> createSuccessful (T o) {
        return fromTry (ATry.success (o));
    }

    static <T> AFuture<T> createFailed (Throwable th) {
        return fromTry (ATry.failure (th));
    }

    static <T> AFuture<T> fromTry (ATry<T> t) {
        return AFutureImpl.fromTry (AThreadPool.SYNC_THREADPOOL, t);
    }

    static <T> AFuture<T> submit (AThreadPool tp, AFunction0<T,?> f) {
        return StaticFutureMethods.submit (tp, f);
    }

    static <T> AFuture<List<T>> lift (Iterable<AFuture<T>> futures) {
        return null; //TODO
    }

    static <T> AFuture<T> firstCompletedOf (Iterable<AFuture<T>> futures) {
        return null; //TODO
    }

    static <T> AFuture<AOption<T>> find (Iterable<AFuture<T>> futures, APredicate<T,?> f) {
        return null; //TODO
    }

    static <T,R> AFuture<R> fold (R start, Iterable<AFuture<T>> futures, AFunction2<R, T, R, ?> f) {
        return null; //TODO
    }

    static <T> AFuture<T> reduce (Iterable<AFuture<T>> futures, AFunction2<T, T, T, ?> f) {
        return null; //TODO
    }

    static <T, R> AFuture<List<R>> traverse (AThreadPool tp, Iterable<T> values, AFunction1<T, R, ?> f) {
        return null; //TODO
    }
}


