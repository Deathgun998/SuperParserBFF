package dao

import java.sql.Connection

trait Connector {

  def connect(propFile: String) : Connection

}
