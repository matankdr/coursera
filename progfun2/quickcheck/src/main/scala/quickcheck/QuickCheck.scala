package quickcheck

import common._
import org.scalacheck._
import Arbitrary._
import Gen._
import Prop._

import scala.annotation.tailrec

abstract class QuickCheckHeap extends Properties("Heap") with IntHeap {

  lazy val genHeap: Gen[H] = for {
    x <- arbitrary[A]
    h <- oneOf(const(empty), genHeap)
  } yield insert(x, h)

  implicit lazy val arbHeap: Arbitrary[H] = Arbitrary(genHeap)

  property("gen1") = forAll { (h: H) =>
    val m = if (isEmpty(h)) 0 else findMin(h)
    findMin(insert(m, h)) == m
  }

  property("delete single element") = forAll { (m: A) =>
    deleteMin(insert(m, empty)) == empty
  }

  property("find min of 2 elements") = forAll {(m1: A, m2: A) =>
    findMin(insert(m2, insert(m1, empty))) == ( m1 min m2 )
  }

  property("elements are ordered") = forAll { (h: H) =>
    def isHeapValid(h: H, last: A): Boolean = {
      if (isEmpty(h)) true
      else if (findMin(h) < last) false
      else isHeapValid(deleteMin(h), findMin(h))
    }

    isHeapValid(deleteMin(h), findMin(h))
  }

  property("contains all inserted elements") = forAll { (elements: List[A]) =>
    def insertElements(h: H, elements: Seq[A]): H = {
      elements match {
        case Nil => h
        case head :: tail => insertElements(insert(head, h), tail)
      }
    }

    def getSize(h: H): Int = {
      def iterate(h: H, accSize: Int): Int = {
        if (isEmpty(h)) accSize
        else iterate(deleteMin(h), accSize + 1)
      }

      iterate(h, 0)
    }

    getSize(insertElements(empty, elements)) == elements.size
  }

  property("min of 2 melded heaps") = forAll { (h1: H, h2: H) =>
    val min1 = findMin(h1)
    val min2 = findMin(h2)
    val melded = meld(h1, h2)

    findMin(melded) == (min1 min min2)
  }

  property("move min from melded heap") = forAll { (h1: H, h2: H) =>
    val meld1 = meld(h1, h2)
    val m1 = findMin(h1)
    val meld2 = meld(deleteMin(h1), insert(m1, h2))

    @tailrec
    def areHeapsEqual(h1: H, h2: H): Boolean = {
      if (isEmpty(h1) && isEmpty(h2)) true
      else {
        val min1 = findMin(h1)
        val min2 = findMin(h2)
        min1 == min2 && areHeapsEqual(deleteMin(h1), deleteMin(h2))
      }
    }

    areHeapsEqual(meld1, meld2)
  }

  property("delete element returns a valid heap") = forAll { (h: H) =>
    def isHeapValid(h: H): Boolean = {
      if (isEmpty(h)) true
      else {
        val min = findMin(h)
        val nextHeap = deleteMin(h)

        isEmpty(nextHeap) ||
          (min <= findMin(nextHeap) && isHeapValid(nextHeap))
      }
    }

    isHeapValid(deleteMin(h))
  }
}
