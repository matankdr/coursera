package actorbintree

import actorbintree.BinaryTreeSet.Insert
import akka.actor.{ActorRef, ActorSystem, Props}

object Main extends App {
  implicit val system = ActorSystem("my-system")
  val root = system.actorOf(Props[BinaryTreeSet])

  root ! Insert(ActorRef.noSender, 100, 100)
  root ! Insert(ActorRef.noSender, 100, 50)
}