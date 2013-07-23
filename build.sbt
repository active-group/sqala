name := "sqala"

TaskKey[Unit]("print-run-classpath") <<= (fullClasspath in Runtime) map { cp => println(cp.files.absString)}
