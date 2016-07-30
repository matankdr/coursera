package reductions

import scala.annotation._
import org.scalameter._
import common._

object ParallelParenthesesBalancingRunner {

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 40,
    Key.exec.maxWarmupRuns -> 80,
    Key.exec.benchRuns -> 120,
    Key.verbose -> true
  ) withWarmer(new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val length = 100000000
    val chars = new Array[Char](length)
    val threshold = 10000
    val seqtime = standardConfig measure {
      seqResult = ParallelParenthesesBalancing.balance(chars)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential balancing time: $seqtime ms")

    val fjtime = standardConfig measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    }
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime ms")
    println(s"speedup: ${seqtime / fjtime}")
  }
}

object ParallelParenthesesBalancing {

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def balance(chars: Array[Char]): Boolean = {
    val result = chars.foldLeft[Int](0) ((agg, c) => c match {
      case _ if agg < 0 => -1
      case ')'          => agg - 1
      case '('          => agg + 1
      case _            => agg
    })

    result == 0
  }

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def parBalance(chars: Array[Char], threshold: Int): Boolean = {

    def traverse(from: Int, until: Int, arg1: Int, arg2: Int) : Option[Int] = {
      if (until - from < threshold) {
        chars.slice(from, until).foldLeft(Option(0))((agg, c) => c match {
          case '(' => agg.map(_ + 1)
          case ')' => agg.map(_ - 1) match {
            case Some(x) if from == 0 && x < 0  => None
            case None => None
            case x => x
          }
          case _ => agg
        })
      } else {
        val pivot = from + (until - from) / 2
        val (resLeft, resRight) = parallel(
          traverse(from, pivot, arg1, arg2),
          traverse(pivot, until, arg1, arg2)
        )

        Option(resLeft.getOrElse(0) + resRight.getOrElse(0))
      }
    }

    def reduce(from: Int, until: Int): Option[Int] = {
      traverse(0, until, 0, until)
    }

    reduce(0, chars.length) == Option(0)
  }

  // For those who want more:
  // Prove that your reduction operator is associative!

}
