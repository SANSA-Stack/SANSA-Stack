package sam
import scala.io.Source
import java.io.{FileReader, FileNotFoundException, IOException}
class t {
  

val filename = "/Home/workspace/chapter03App/3_signal_anonym_directed_v3.txt"
try {
for (line <- Source.fromFile(filename).getLines()) {
  println(line)
}
} catch {
  case ex: FileNotFoundException => println("Couldn't find that file.")
  case ex: IOException => println("Had an IOException trying to read that file")
}
}