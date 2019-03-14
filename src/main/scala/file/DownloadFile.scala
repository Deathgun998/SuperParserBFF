package file

import java.io._
import java.net.{HttpURLConnection, URL}
import java.nio.charset.StandardCharsets
import java.util.zip._

import scala.io.Source

class DownloadFile {

  def decompressGZ(fileName: String, destination: String): Unit ={


    var in   = new GZIPInputStream(new FileInputStream(fileName))
    var fos  = new FileOutputStream(destination)
    var w    = new PrintWriter(new OutputStreamWriter(
      fos, StandardCharsets.UTF_8),true)
    for (line: String <- Source.fromInputStream(in).getLines()) {
      w.write(line+"\n")
    }
    w.close()
    fos.close()
  }

  def downloadFile(toDownload: String, destination: String) {

    val url = new URL(toDownload)
    val connection = url.openConnection().asInstanceOf[HttpURLConnection]
    connection.setRequestMethod("GET")

    val responseurl = connection.getHeaderField("Location")
    val status = connection.getResponseCode()
    println(status)
    if (status == HttpURLConnection.HTTP_OK){
      val in: InputStream = connection.getInputStream
      val fileToDownloadAs = new java.io.File(destination)
      val out: OutputStream = new BufferedOutputStream(new FileOutputStream(fileToDownloadAs))
      val byteArray = Stream.continually(in.read).takeWhile(-1 !=).map(_.toByte).toArray
      out.write(byteArray)
    } else {
      if (status == HttpURLConnection.HTTP_MOVED_TEMP
        || status == HttpURLConnection.HTTP_MOVED_PERM
        || status == HttpURLConnection.HTTP_SEE_OTHER)
        {

          val newUrl  = new URL(responseurl)
          val redirConnection = newUrl.openConnection().asInstanceOf[HttpURLConnection]
          redirConnection.setRequestMethod("GET")
          redirConnection.addRequestProperty("User-Agent", "Mozilla");
          println("scarico dal nuovo url")
          println(redirConnection.getResponseCode())
          val in: InputStream = redirConnection.getInputStream
          val fileToDownloadAs = new java.io.File(destination)

          val out: OutputStream = new BufferedOutputStream(new FileOutputStream(fileToDownloadAs))
          Iterator
            .continually (in.read)
            .takeWhile (-1 !=)
            .foreach (out.write)
          out.flush()
          out.close()
        }
    }


  }

  def deleteFile(path: String):Boolean ={
    return new File(path).delete()
  }
}
