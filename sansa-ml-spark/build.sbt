
name          := "SparkScalaWordNet"

organization  := "sda"

version       := "0.1.0-SNAPSHOT"

scalaVersion  := "2.11.7"

scalacOptions ++= Seq("-deprecation", "-unchecked", "-feature", "-encoding", "utf8", "-Xfuture")

licenses      := Seq("Apache v2" -> url("https://www.apache.org/licenses/LICENSE-2.0"))

libraryDependencies ++= Seq(
  "net.sf.jwordnet" % "jwnl"            % "1.4_rc3"
)
unmanagedClasspath in Runtime += baseDirectory.value / "src/main/config"
unmanagedClasspath in Test    += baseDirectory.value / "src/main/config"
unmanagedClasspath in Compile += baseDirectory.value / "src/main/config"

lazy val `download-database` = taskKey[Unit]("Download the word-net database and installation to config and link")

`download-database` := {
  val configDir = file("config")
  val dbFile    = configDir / "wnjpn.db"
  val st        = streams.value
  if (dbFile.exists()) {
    st.log.info(s"Database file ${dbFile.name} already present.")
  } else {
    st.log.info("Downloading database...")
    IO.withTemporaryFile(prefix = "wnjpn.db", postfix = "gz") { tmpFile =>
      IO.download(new URL("http://compling.hss.ntu.edu.sg/wnja/data/1.1/wnjpn.db.gz"), tmpFile)
      IO.gunzip(tmpFile, dbFile)
    }
  }
  val linkDir = file("link")
  val wnFile  = linkDir / "WordNet-3.0"
  if (wnFile.exists()) {
    st.log.info(s"WordNet installation ${wnFile.name} already present.")
  } else {
    st.log.info("Downloading WordNet...")
    IO.withTemporaryFile(prefix = "WordNet", postfix = "gz") { tmpFile =>
      IO.download(new URL("http://wordnetcode.princeton.edu/3.0/WordNet-3.0.tar.gz"), tmpFile)
      Seq("tar", "-xf", tmpFile.getPath, "-C", linkDir.getPath).!
    }
  }
}
