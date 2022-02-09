var c = Tuple1(12,"Chetan")

c._1

var a:List[Any] = List("asd","",5.7)

var map = scala.collection.mutable.Map(1->"Chetan")

map.put(2,"Harshu")

map(2)

def doubler(a:Int) = a*2

val abc = doubler(_)

abc(3)

val pi = 2.14
def areaOfCircle(pi: Double) = {
  val pi = 3.14
  x:Int => x*x*pi
}

areaOfCircle(pi)(10)

val r = 1 to 10

r foreach println

r + "avc"

r foreach println
