package edu.stanford.nlp.smr;

object Util {
  def identity[T] = (x : T)  => x;
  def fMap[T,U](f : T=>U) : (Iterable[T])=>Iterable[U] = (x : Iterable[T]) => x.map(f);
  def fFlatMap[T,U](f : T=>Iterable[U]) : (Iterable[T])=>Iterable[U] = (x : Iterable[T]) => x.flatMap(f);
  def fFilter[T](f : T=>Boolean) : (Iterable[T])=>Iterable[T] = (x : Iterable[T]) => x.filter(f);
}
