package calculator

object Polynomial {
  def computeDelta(a: Signal[Double], b: Signal[Double],
      c: Signal[Double]): Signal[Double] = {
    Signal(b() * b() - 4 * a() * c() )
  }

  def computeSolutions(a: Signal[Double], b: Signal[Double],
      c: Signal[Double], delta: Signal[Double]): Signal[Set[Double]] = {

    Signal {
      if (delta() < 0) Set.empty[Double]
      else if (a() == 0) Set.empty[Double]
      else {
        Set( -Math.sqrt(delta()), Math.sqrt(delta()) )
          .map(d => ( -b() + d) / (2*a()) )
      }
    }

  }
}
