import java.io.{BufferedOutputStream, FileOutputStream}
import java.nio.file.{Files, Paths}




val arrayByte = Array[Byte](Integer.parseInt("6D",16).toByte,Integer.parseInt("61",16).toByte,Integer.parseInt("72",16).toByte,Integer.parseInt("69",16).toByte,Integer.parseInt("6F",16).toByte)
val bos = new BufferedOutputStream(new FileOutputStream("/home/dap/prova"))
Stream.continually(bos.write(arrayByte))
bos.close()




/*val byteArray = Files.readAllBytes(Paths.get("/home/dap/prova"))

byteArray.foreach(item => item.toString)

byteArray */

