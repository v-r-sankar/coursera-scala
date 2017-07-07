package recfun

object Main {
  def main(args: Array[String]) {
    //    println("Pascal's Triangle")
    //    for (row <- 0 to 10) {
    //      for (col <- 0 to row)
    //        print(pascal(col, row) + " ")
    //      println()
    //    }

    println(countChange(6, List(1,2,4)))
    println(countChange(300,List(5,10,20,50,100,200,500)))

  }

  /**
   * Exercise 1
   */
  def pascal(c: Int, r: Int): Int = {
    if (r == 0) 1
    else if (c == 0 || c == r) 1
    else pascal(c - 1, r - 1) + pascal(c, r - 1)
  }

  /**
   * Exercise 2
   */
  def balance(chars: List[Char]): Boolean = {
    def matchBraches(chars: List[Char], count: Int): Int = {
      if (chars.isEmpty) {
        count
      } else {
        chars.head match {
          case '(' => matchBraches(chars.tail, count + 1)
          case ')' => if (count < 1) matchBraches(List[Char](), 1) else matchBraches(chars.tail, count - 1)
          case _   => matchBraches(chars.tail, count)
        }
        //        if (chars.head == '(')
        //          matchBraches(chars.tail, count + 1)
        //        else if (chars.head == ')') {
        //          if (count < 1) matchBraches(List[Char](), 1)
        //          else
        //            matchBraches(chars.tail, count - 1)
        //        } else {
        //           matchBraches(chars.tail, count )
        //        }
      }
    }
    matchBraches(chars, 0) == 0
  }

  /**
   * Exercise 3
   */
  /* def countChange(money: Int, coins: List[Int]): Int = {
    val localCoins = coins.sortBy(x => x)
    var validCombs = List[List[Int]]()
    def countChangeRecr(money: Int, combs: List[Int]): Int = {
      var count = 0;
      var idx = 0
      while (idx < localCoins.size) {
        val denomination = localCoins(idx)
        if (denomination == money) {
          count = count + 1
          var tmp = combs :+ denomination
          tmp = tmp.sortBy(x=>x)
//          println(tmp)
          validCombs = validCombs :+ tmp
//          println(validCombs)
        } else if (money > denomination) {
          count = count + countChangeRecr(money - denomination,(combs :+ denomination))
        } else {
          idx = localCoins.size
        }
        idx = idx + 1
      }
//      validCombs.map(x => x.sortBy(y=>y))
     
      count
    }
    
    countChangeRecr(money, List[Int]())
    validCombs.distinct.size
    
  }*/

  def countChange(money: Int, coins: List[Int]): Int = {

    def countChangeRecr(money: Int, coins: List[Int]): Int = {
      var count = 0;
      var idx = 0
      while (idx < coins.size) {
        val denomination = coins(idx)
        if (money < denomination) idx = coins.size
        else {
          if (money % denomination == 0) count = count + 1
          val multfactor = money / denomination - 1
          var j = 1
          while (j < multfactor) {
            count = count + countChangeRecr(money - denomination * j, coins.drop(idx+1))
            j = j + 1
          }
        }
        idx = idx + 1
      }
      count
    }
    val localCoins = coins.sortBy(x => x)
    countChangeRecr(money, localCoins)
  }
}
