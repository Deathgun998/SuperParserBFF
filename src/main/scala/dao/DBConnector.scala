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

    val sql_url = dbProp.getProperty("sql.url")
    val sql_user = dbProp.getProperty("sql.user")
    val sql_password = dbProp.getProperty("sql.password")
    val driver_name = dbProp.getProperty("sql.driver.name")

    Class.forName(driver_name)
    val sql_connection: Connection = DriverManager.getConnection(sql_url, sql_user, sql_password)

    return sql_connection
  }

  def saveOnDB(df: DataFrame, table: String): Unit ={
    val dbConfigFile = propFile
    val dbProp = new Properties()
    dbProp.load(new FileInputStream(dbConfigFile))

    val sql_url = dbProp.getProperty("sql.url")
    val sql_user = dbProp.getProperty("sql.user")
    val sql_password = dbProp.getProperty("sql.password")
    val driver_name = dbProp.getProperty("sql.driver.name")

    Class.forName(driver_name)

    df.write.mode(SaveMode.Append).jdbc(sql_url,table,dbProp)
  }
}
