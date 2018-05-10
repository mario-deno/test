import scala.util.{Failure, Success}

val throwable = new Throwable("cazzo sono un errore")
val a = Failure(throwable)

val b = Success("1")



//a.get //-Throwable
a.failed.get
//b.get
b.failed.get





