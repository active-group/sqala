package de.ag.sqala

import Aliases.Environment

object Environment {
  def make(bindings: (String, Type)*): Environment =
    bindings.toMap
}
