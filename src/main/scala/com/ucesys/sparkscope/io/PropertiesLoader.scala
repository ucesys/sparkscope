package com.ucesys.sparkscope.io

import java.util.Properties

trait PropertiesLoader {
  def load(): Properties
}
