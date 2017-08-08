package funsets

/**
 * 2. Purely Functional Sets.
 */
object FunSets {
  /**
   * We represent a Set by its characteristic function, i.e.
   * its `contains` predicate.
   */
  type Set = Int => Boolean

  /**
   * Indicates whether a Set contains a given element.
   */
  def contains(s: Set, elem: Int): Boolean = s(elem)

  /**
   * Returns the Set of the one given element.
   */
  def singletonSet(elem: Int): Set = (x: Int) => (x == elem)

  /**
   * Returns the union of the two given Sets,
   * the Sets of all elements that are in either `s` or `t`.
   */
  def union(s: Set, t: Set): Set = (elem: Int) => (contains(s, elem) || contains(t, elem))

  /**
   * Returns the intersection of the two given Sets,
   * the Set of all elements that are both in `s` and `t`.
   */
  def intersect(s: Set, t: Set): Set = (elem: Int) => (contains(s, elem) && contains(t, elem))

  /**
   * Returns the difference of the two given Sets,
   * the Set of all elements of `s` that are not in `t`.
   */
  def diff(s: Set, t: Set): Set = (elem: Int) => (contains(s, elem) && !contains(t, elem))

  /**
   * Returns the subSet of `s` for which `p` holds.
   */
  def filter(s: Set, p: Int => Boolean): Set = intersect(p, s)

  /**
   * The bounds for `forall` and `exists` are +/- 1000.
   */
  val bound = 1000

  /**
   * Returns whether all bounded integers within `s` satisfy `p`.
   */
  def forall(s: Set, p: Int => Boolean): Boolean = {
    def iter(a: Int): Boolean = {
      if (a > bound) true
      else if (contains(s,a) && !contains(p, a)) false
      else iter(a+1)
    }
    iter(-bound)
  }

  /**
   * Returns whether there exists a bounded integer within `s`
   * that satisfies `p`.
   */
  def exists(s: Set, p: Int => Boolean): Boolean = {
     def iter(a: Int): Boolean = {
      if (a > bound) false
      else if ( contains(s,a) && contains(p, a)) true
      else iter(a+1)
    }
    iter(-bound)
  }

  /**
   * Returns a Set transformed by applying `f` to each element of `s`.
   */
  def map(s: Set, f: Int => Int): Set = (x :Int) => s(f(x))

  /**
   * Displays the contents of a Set
   */
  def toString(s: Set): String = {
    val xs = for (i <- -bound to bound if contains(s, i)) yield i
    xs.mkString("{", ",", "}")
  }

  /**
   * Prints the contents of a Set on the console.
   */
  def printSet(s: Set) {
    println(toString(s))
  }
}
