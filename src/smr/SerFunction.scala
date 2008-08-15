package smr;

@serializable trait SerFunction0[+B] extends Function0[B];
@serializable trait SerFunction1[-A,+B] extends Function1[A,B];
@serializable trait SerFunction2[-A,-B,+C] extends Function2[A,B,C];
@serializable trait SerFunction3[-A,-B,-C,+D] extends Function3[A,B,C,D];


// please ANT
object SerFunction;
