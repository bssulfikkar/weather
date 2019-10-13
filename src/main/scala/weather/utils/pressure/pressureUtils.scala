package scala.weather.utils.pressure

import java.util.Properties
import scala.io.Source

/**
 * Utility methods class related to property file
 *
 * @author Sulfikkar Basheer Shylaja
 * @version 1.0
 */
class pressureUtils {

  /**
   * Reads property file
   *
   * @return Properties
   */
  def readPropertyFile(): Properties = {
    var properties: Properties = null
    val url = getClass.getResource("/properties/pressure/pressureconfig.properties")
    if (url != null) {
      val source = Source.fromURL(url)
      properties = new Properties()
      properties.load(source.bufferedReader())
    }
    properties
  }
}