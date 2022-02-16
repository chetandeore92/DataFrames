


/*def toInt(str: String) :Option[Int] = {
 try{
   Some(str.toInt)
 }
  catch {
    case ex : NumberFormatException => None
  }
}

toInt("234").getOrElse("Exception was raised")

toInt("ddd").getOrElse("Exception was raised")

toInt("-1").getOrElse("Exception was raised")

val list = List("1","3","abc","4","e")

list.flatMap(toInt).sum*/


/*
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

def printff(str : Float *) = {
  str.foreach(println)
}

printff(44.5f,44)

 null
*/

def transformInt(a: Int,b:Int):Int =
{
  a*b
}

def trippler(x: Int,f:(Int)=> Int)  ={
  f(x)
}

trippler(10,x=>x*3)
