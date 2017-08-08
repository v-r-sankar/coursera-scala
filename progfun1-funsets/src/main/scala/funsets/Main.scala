package funsets


object Main extends App {
  import FunSets._
  
  println(contains(singletonSet(1), 1))
  val s1  = union(singletonSet(1),singletonSet(3));
  val s2  = singletonSet(1);
  val s3  = singletonSet(3);
  
  println(forall(s1, s2))
  println(exists(s1, s2))
  
  printSet(map(s1,(x:Int)=>(x+1)))
}
