package com.ucesys.sparkscope.io.property

import java.io.FileInputStream
import java.util.Properties

class LocalPropertiesLoader(propertiesPathStr: String) extends PropertiesLoader {
    def load(): Properties = {
        val prop = new Properties()
        prop.load(new FileInputStream(propertiesPathStr));
        prop
    }
}
