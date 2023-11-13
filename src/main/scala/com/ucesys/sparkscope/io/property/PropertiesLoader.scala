package com.ucesys.sparkscope.io.property

import java.util.Properties

trait PropertiesLoader {
    def load(): Properties
}
