package com.reactivebbq.orders

import java.util.UUID

import akka.actor.{Actor, ActorLogging, Props, Stash, Status}
import akka.cluster.sharding.ShardRegion
import akka.cluster.sharding.ShardRegion.{ExtractEntityId, ExtractShardId}

import scala.concurrent.Future
import akka.pattern._

object OrderActor {
  sealed trait Command extends SerializableMessage

  case class OpenOrder(server: Server, table: Table) extends Command
  case class OrderOpened(order: Order) extends SerializableMessage
  case class AddItemToOrder(item: OrderItem) extends Command
  case class ItemAddedToOrder(order: Order) extends SerializableMessage
  case class GetOrder() extends Command

  case class Envelope(orderId: OrderId, command: Command) extends SerializableMessage

  case class OrderNotFoundException(orderId: OrderId) extends IllegalStateException(s"Order Not Found: $orderId")
  case class DuplicateOrderException(orderId: OrderId) extends IllegalStateException(s"Duplicate Order: $orderId")

  private case class OrderLoaded(order: Option[Order])

  def props(orderRepository: OrderRepository): Props = Props(new OrderActor(orderRepository))

  val entityIdExtractor: ExtractEntityId = {
    case Envelope(OrderId(value), command) => value.toString -> command
  }

  val shardIdExtractor: ExtractShardId = {
    case Envelope(OrderId(value), command) => Math.abs(value.toString.hashCode % 30).toString
    case ShardRegion.StartEntity(entityId) => Math.abs(entityId.hashCode % 30).toString
  }
}

class OrderActor(orderRepository: OrderRepository) extends Actor with ActorLogging with Stash{
  import context.dispatcher
  import OrderActor._

  private val orderId = OrderId(UUID.fromString(context.self.path.name))
  private var state: Option[Order] = None

  orderRepository.find(orderId)
    .map(OrderLoaded.apply)
    .pipeTo(self)

  private def openOrder(orderId: OrderId, server: Server, table: Table): Future[OrderOpened] = {
    orderRepository
      .update(Order(orderId, server, table, Seq()))
      .map(OrderOpened)
  }

  private def duplicateOrder[T](orderId: OrderId): Future[T] = {
    Future.failed(DuplicateOrderException(orderId))
  }

  private def addItem(order: Order, orderItem: OrderItem): Future[ItemAddedToOrder] = {
    orderRepository
      .update(order.withItem(orderItem))
      .map(ItemAddedToOrder)
  }

  private def orderNotFound[T](orderId: OrderId): Future[T] = {
    Future.failed(OrderNotFoundException(orderId))
  }

  override def receive: Receive = loading
//  override def receive: Receive = {
//    case OpenOrder(server, table) =>
//      log.info(s"[$orderId] OpenOrder($server, $table)")
//
//      orderRepository.find(orderId).flatMap {
//        case Some(_) => duplicateOrder(orderId)
//        case None => openOrder(orderId, server, table)
//      }.pipeTo(sender())
//
//    case AddItemToOrder(item) =>
//      log.info(s"[$orderId] AddItemToOrder($item)")
//
//      orderRepository.find(orderId).flatMap {
//        case Some(order) => addItem(order, item)
//        case None        => orderNotFound(orderId)
//      }.pipeTo(sender())
//
//    case GetOrder() =>
//      log.info(s"[$orderId] GetOrder()")
//
//      orderRepository.find(orderId).flatMap {
//        case Some(order) => Future.successful(order)
//        case None => orderNotFound((orderId))
//      }.pipeTo(sender())
//  }



  private def loading: Receive = {
    case OrderLoaded(order) =>
      unstashAll()
      state = order
      context.become(running)

    case Status.Failure(err) =>
      log.error(s"[$orderId] FAILURE: ${err.getMessage}")

      throw err

    case _ =>
      stash()
  }

  private def running: Receive = {
    case OpenOrder(server, table) =>
      log.info(s"[$orderId] OpenOrder($server, $table)")

      state match {
        case Some(_) =>
          duplicateOrder(orderId).pipeTo(sender())

        case None =>
          context.become(waiting)
          openOrder(orderId, server, table).pipeTo(self)(sender())
      }

    case AddItemToOrder(item) =>
      state match {
        case Some(order) =>
          context.become(waiting)
          addItem(order, item).pipeTo(self)(sender())
        case None =>
          orderNotFound(orderId).pipeTo(sender())
      }

    case GetOrder() =>
      log.info(s"[$orderId] GetOrder()")

      state match {
        case Some(order) => sender() ! order
        case None => orderNotFound(orderId).pipeTo(sender())
      }
  }

  private def waiting: Receive = {
    case OrderOpened(order) =>
      state = Some(order)
      unstashAll()
      sender() ! OrderOpened(order)
      context.become(running)

    case ItemAddedToOrder(order) =>
      state = Some(order)
      unstashAll()
      sender() ! ItemAddedToOrder(order)
      context.become(running)

    case failure @ Status.Failure(err) =>
      log.error(s"[$orderId] FAILURE: ${err.getMessage}")

      sender() ! failure
      throw err

    case _ =>
      stash()
  }
}
