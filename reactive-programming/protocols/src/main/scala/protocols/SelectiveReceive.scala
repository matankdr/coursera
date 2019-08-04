package protocols

import akka.actor.typed.scaladsl._
import akka.actor.typed.{ActorContext, _}

object SelectiveReceive {
    /**
      * @return A behavior that stashes incoming messages unless they are handled
      *         by the underlying `initialBehavior`
      * @param bufferSize Maximum number of messages to stash before throwing a `StashOverflowException`
      *                   Note that 0 is a valid size and means no buffering at all (ie all messages should
      *                   always be handled by the underlying behavior)
      * @param initialBehavior Behavior to decorate
      * @tparam T Type of messages
      *
      * Hint: Implement an [[ExtensibleBehavior]], use a [[StashBuffer]] and [[Behavior]] helpers such as `start`,
      * `validateAsInitial`, `interpretMessage`,`canonicalize` and `isUnhandled`.
      */
    def apply[T](bufferSize: Int, initialBehavior: Behavior[T]): Behavior[T] = {
      new Selective(initialBehavior, StashBuffer[T](bufferSize), bufferSize)
    }

  class Selective[T](behavior: Behavior[T], buffer: StashBuffer[T], bufferSize: Int) extends ExtensibleBehavior[T] {
    import akka.actor.typed.Behavior.{canonicalize, interpretMessage, start, validateAsInitial, interpretSignal}

    override def receive(ctx: ActorContext[T], msg: T): Behavior[T] = {

      val started = validateAsInitial(start(behavior, ctx))
      val next = interpretMessage(started, ctx, msg)
      val nextCanonicalized = canonicalize(next, started, ctx)

      if (Behavior.isUnhandled(next)) {
        buffer stash msg
        new Selective(nextCanonicalized, buffer, bufferSize)
      } else if (buffer.nonEmpty) {
        buffer.unstashAll(ctx.asScala, new Selective(nextCanonicalized, StashBuffer(bufferSize), bufferSize))
      } else {
        new Selective(nextCanonicalized, buffer, bufferSize)
      }
    }

    override def receiveSignal(ctx: ActorContext[T], signal: Signal): Behavior[T] = {
      val started = validateAsInitial(start(behavior, ctx))
      val next = interpretSignal(started, ctx, signal)
      val nextCanonicalized = canonicalize(next, started, ctx)
      
      new Selective(nextCanonicalized, buffer, bufferSize)
    }
  }
}
