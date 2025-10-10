package org.apache.iceberg

package object spark {
  def supportsCatalog(catalogClass: Class[_]): Boolean = {
    classOf[BaseCatalog].isAssignableFrom(catalogClass)
  }
}
