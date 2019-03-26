package config.application

import java.io.FileInputStream
import java.util.Properties

class ApplicationConfig {

  def loadAppConf():Properties={
    val configFile = ""

    val props = new Properties()

    props.load(new FileInputStream(configFile))

    props;
  }

  def getFileUrl():String ={
    val props = loadAppConf()

    val filepath = props.getProperty("url")

    return filepath
  }

  def getFile2Download(): String = {
    val props = loadAppConf()

    val date = props.getProperty("data")

    val fileNumber = props.getProperty("file.number")

    val extension = props.getProperty("extension")

    val filename = date + "-" + fileNumber + extension

    return filename
  }

  def getFileDestination(): String ={
    val props = loadAppConf()

    val destination = props.getProperty("destination")

    destination
  }

}
