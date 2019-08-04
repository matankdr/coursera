/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package actorbintree

import actorbintree.BinaryTreeNode.{CopyFinished, CopyTo}
import akka.actor._
import akka.event.LoggingReceive

import scala.collection.immutable.Queue

object BinaryTreeSet {

  trait Operation {
    def requester: ActorRef
    def id: Int
    def elem: Int
  }

  trait OperationReply {
    def id: Int
  }

  /** Request with identifier `id` to insert an element `elem` into the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to check whether an element `elem` is present
    * in the tree. The actor at reference `requester` should be notified when
    * this operation is completed.
    */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to remove the element `elem` from the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to perform garbage collection*/
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply
  
  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply

}


class BinaryTreeSet extends Actor {
  import BinaryTreeSet._

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root = createRoot

  // optional
  var pendingQueue = Queue.empty[Operation]

  // optional
  def receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = {
    case insert:Insert =>      root ! insert
    case contains: Contains => root ! contains
    case remove: Remove =>     root ! remove
    case GC =>
      println("I'm root, received GC")
      val newRoot = createRoot
      context.become(garbageCollecting(newRoot))
      root ! CopyTo(newRoot)
  }

//  val onGC: Receive = {
//    case GC =>
//      // ignore GC requests while on GC
//    case CopyFinished =>
//      println("I'm root, received CopyFinished")
//      newRoot.foreach { nr =>
//        root = nr
//        newRoot = None
//      }
//
//      context.become(normal)
//      pendingQueue.foreach(root ! _)
//      pendingQueue = Queue.empty[Operation]
//
//    case x: Operation =>
//      println(s"message ${x} added to queue while GC")
//      pendingQueue = pendingQueue enqueue  x
//  }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = {
    case GC =>
    // ignore GC requests while on GC

    case CopyFinished =>
      println("I'm root, received CopyFinished")
      root ! PoisonPill
      root = newRoot
      context.become(normal)
      pendingQueue.foreach(root ! _)
      pendingQueue = Queue.empty[Operation]

    case x: Operation =>
      println(s"message ${x} added to queue while GC")
      pendingQueue = pendingQueue enqueue  x
  }

}

object BinaryTreeNode {
  trait Position

  case object Left extends Position
  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)
  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode],  elem, initiallyRemoved)
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor {
  import BinaryTreeNode._
  import BinaryTreeSet._

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  def createNewNode(elem: Int) = context.actorOf(BinaryTreeNode.props(elem, initiallyRemoved = false))

  // optional
  def receive = normal

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  def normal: Receive = {
    case Insert(requester, id, newElem) if newElem > elem =>
      println(s"DEBUG [INSERT][$id]: $newElem > $elem")
      if (subtrees.contains(Right)) {
        println(s"DEBUG [INSERT][$id]: contains Right")
        subtrees(Right) ! Insert(requester, id, newElem)
      } else {
        println(s"DEBUG [INSERT][$id]: insert $newElem")
        val newNode = createNewNode(newElem)
        subtrees += (Right -> newNode)
        requester ! OperationFinished(id)
      }

    case Insert(requester, id, newElem) if newElem < elem =>
      println(s"DEBUG [INSERT][$id] $newElem < $elem")
      if (subtrees.contains(Left)) {
        println(s"DEBUG [INSERT][$id]: contains Left")
        subtrees(Left) ! Insert(requester, id, newElem)
      } else {
        println(s"DEBUG [INSERT][$id]: insert $newElem")
        val newNode = createNewNode(newElem)
        subtrees += (Left -> newNode)
        requester ! OperationFinished(id)
      }

    case Insert(requester, id, newElem) =>
      if (newElem == elem) {
        this.removed = false
        println(s"DEBUG [INSERT][$id]: $newElem is now added")
      }
      println(s"DEBUG [INSERT][$id]: arrived to existing $elem")
      requester ! OperationFinished(id)

    case Remove(requester, id, elemToRemove) =>
      if (elemToRemove > elem && subtrees.contains(Right)) {
        println(s"DEBUG [REMOVE][$id]: $elemToRemove > $elem and RIGHT")
        subtrees(Right) ! Remove(requester, id, elemToRemove)
      } else if (elemToRemove < elem && subtrees.contains(Left)){
        println(s"DEBUG [REMOVE][$id]: $elemToRemove < $elem and LEFT")
        subtrees(Left) ! Remove(requester, id, elemToRemove)
      } else {
        if (elemToRemove == elem) {
          this.removed = true
        }
        println(s"DEBUG [REMOVE][$id]: $elemToRemove (==|!=) $elem")
        requester ! OperationFinished(id)
      }

    case Contains(requester, id, elemToFind) =>
      if (elemToFind > elem && subtrees.contains(Right)) {
        println(s"DEBUG [CONTAINS][$id]: $elemToFind > $elem and RIGHT")
        subtrees(Right) ! Contains(requester, id, elemToFind)
      } else if (elemToFind < elem && subtrees.contains(Left)) {
        println(s"DEBUG [CONTAINS][$id]: $elemToFind < $elem and LEFT")
        subtrees(Left) ! Contains(requester, id, elemToFind)
      } else if (elemToFind == elem && !this.removed) {
        println(s"DEBUG [CONTAINS][$id]: $elemToFind == $elem contains=true")
        requester ! ContainsResult(id, true)
      } else {
        println(s"DEBUG [CONTAINS][$id]: $elemToFind != $elem, contains=false")
        requester ! ContainsResult(id, false)
      }

    case CopyTo(treeNode) =>
      println(s"[CopyTo] I'm $elem")

      subtrees.foreach{ case (_, node) => node ! CopyTo(treeNode) }

      if (!this.removed) {
        treeNode ! Insert(self, elem, elem)
        println(s"[CopyTo] $elem to newNode")
      }

      if (subtrees.isEmpty) {
        sender() ! CopyFinished
      } else {
        context.become(copying(subtrees.values.toSet, false, sender()))
      }

    case OperationFinished(_) =>

    case x =>
      println(s"I'm $elem: received message $x, and I'm not ready for it")
      ???
  }

//  def onGC(pendingCopyResponses: Int, treeNode: ActorRef): Receive = {
//    case CopyFinished if pendingCopyResponses == 0 =>
//      println(s"[CopyFinished == 0]: [in $elem] pending=$pendingCopyResponses")
//      context.become(normal)
//      println(s"in $elem, send CopyFinished")
//      treeNode ! CopyFinished
//
//    case CopyTo(treeNode) =>
//      println(s"[CopyTo]: [in $elem] pending=$pendingCopyResponses")
//      subtrees.foreach{ case (_, node) => node ! CopyTo(treeNode) }
//
//      if (!this.removed) {
//        println(s"[CopyTo]: [in $elem] copying $elem to new tree")
//        treeNode ! Insert(self, elem, elem)
//      }
//
//      self ! CopyFinished
//
//    case CopyFinished =>
//      println(s"[CopyFinished]: [in $elem] pending=$pendingCopyResponses")
//      context.become(onGC(pendingCopyResponses - 1, treeNode))
//      println(s"[I'm $elem] waiting for ${pendingCopyResponses - 1} responses")
//  }

  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean, requester: ActorRef): Receive = {
//    case CopyTo(treeNode) =>
//      subtrees.foreach{ case (_, node) => node ! CopyTo(treeNode) }

    case CopyFinished if expected.size == 1 =>
      println(s"DEBUG [CopyFinished] [$elem] expected.size == 0")
      context.become(copying(expected = expected, insertConfirmed = true, requester))
      println(s"[CopyFinished] of $elem")
      requester ! CopyFinished

    case CopyFinished =>
      println(s"DEBUG [CopyFinished] [$elem] expected.size == ${expected.size}")
      context.become(copying(expected = expected - sender(), insertConfirmed = false, requester))
  }
}
