import java.io._
import java.nio.charset.StandardCharsets
import java.util.zip._
import sys.process._
import java.net.URL

import scala.io.{ Source}

class DownloadFile {

  def downloadFile(toDownload: String, destination: String): Unit = {

    new URL(toDownload) #> new File(destination) !!
  }

  def decompressGZ(fileName: String, destination: String): Unit ={


    var in   = new GZIPInputStream(new FileInputStream(fileName))
    var fos  = new FileOutputStream(destination)
    var w    = new PrintWriter(new OutputStreamWriter(
      fos, StandardCharsets.UTF_8),true)
    for (line: String <- Source.fromInputStream(in).getLines()) {
      println(line)
      w.write(line+"\n")
    }
    w.close()
    fos.close()
  }
}
