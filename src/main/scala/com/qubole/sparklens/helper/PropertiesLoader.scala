package com.qubole.sparklens.helper

import java.util.Properties

trait PropertiesLoader {
  def load(propertiesPath: String): Properties
}
