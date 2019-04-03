package dao
import java.io.FileInputStream
import java.sql.{Connection, DriverManager}
import java.util.Properties

import org.apache.spark.sql.{DataFrame, SaveMode}

class DBConnector(propFile: String) extends Connector {
  override def connect(): Connection = {

    val dbConfigFile = propFile
    val dbProp = new Properties()
    dbProp.load(new FileInputStream(dbConfigFile))

    val sql_url = dbProp.getProperty("url")
    val sql_user = dbProp.getProperty("user")
    val sql_password = dbProp.getProperty("password")
    val driver_name = dbProp.getProperty("Driver")

    Class.forName(driver_name)
    val sql_connection: Connection = DriverManager.getConnection(sql_url, sql_user, sql_password)

    return sql_connection
  }

  def saveOnDB(df: DataFrame, table: String): Unit ={
    val dbConfigFile = propFile
    val dbProp = new Properties()
    dbProp.load(new FileInputStream(dbConfigFile))

    val sql_url = dbProp.getProperty("url")
    val sql_user = dbProp.getProperty("user")
    val sql_password = dbProp.getProperty("password")
    val driver_name = dbProp.getProperty("Driver")

    Class.forName(driver_name)

    /*val connectionProperties = new Properties()

    connectionProperties.put("user", s"${sql_user}")
    connectionProperties.put("password", s"${sql_password}")

    df.write.mode(SaveMode.Overwrite).jdbc(sql_url,table,connectionProperties)

    println("salvato")
*/

    val url: String = dbProp.getProperty("url")
    val tableName: String = table
    val user: String = dbProp.getProperty("user")
    val password: String = dbProp.getProperty("password")
    val properties = new Properties()
    properties.setProperty("user", user)
    properties.setProperty("password", password)
    properties.put("driver", "org.postgresql.Driver")
    df.write.mode(SaveMode.Overwrite).jdbc(url, tableName, properties)
  }
}
