package net.sansa_stack.datalake.spark

import com.typesafe.config.ConfigFactory

/**
  * Created by mmami on 04.08.17.
  */
class Config {

}
object Config {

    def get(key: String): String = {

        val value = ConfigFactory.load().getString(key)

        return value
    }
}
