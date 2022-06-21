package com.reactivebbq.orders

import java.util.UUID

import akka.actor.ActorRef
import akka.http.scaladsl.model.{HttpResponse, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{ExceptionHandler, Route}
import akka.util.Timeout
import com.reactivebbq.orders.OrderActor.{Envelope, GetOrder, ItemAddedToOrder, OpenOrder, OrderOpened}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import akka.pattern.ask

class OrderRoutes(orderActors: ActorRef)(implicit ec: ExecutionContext)
  extends OrderJsonFormats {

  private implicit val timeout: Timeout = Timeout(5.seconds)

  private def exceptionHandler: ExceptionHandler = ExceptionHandler {
    case ex: OrderActor.OrderNotFoundException =>
      complete(HttpResponse(StatusCodes.NotFound, entity = ex.getMessage))
    case ex =>
      complete(HttpResponse(StatusCodes.InternalServerError, entity = ex.getMessage))
  }

  lazy val routes: Route =
    handleExceptions(exceptionHandler) {
      pathPrefix("order") {
        post {
          entity(as[OrderActor.OpenOrder]) { openOrder =>
            complete {
              val orderId = OrderId()

              (orderActors ? Envelope(orderId, openOrder))
                .mapTo[OrderOpened]
                .map(_.order)
            }
          }
        } ~
        pathPrefix(Segment) { id =>

          val orderId = OrderId(UUID.fromString(id))

          get {
            complete {
              (orderActors ? Envelope(orderId, GetOrder()))
                .mapTo[Order]
            }
          } ~
          path("items") {
            post {
              entity(as[OrderActor.AddItemToOrder]) { addItemToOrder =>
                complete {
                  (orderActors ? Envelope(orderId, addItemToOrder))
                    .mapTo[ItemAddedToOrder]
                    .map(_.order)
                }
              }
            }
          }
        }
      }
    }
}
