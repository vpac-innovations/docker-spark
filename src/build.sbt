name         := "SparkMe Project"
version      := "1.0"
organization := "pl.japila"

scalaVersion := "2.11.7"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.0.0"
libraryDependencies += "edu.ucar" % "cdm" % "4.5.5"
libraryDependencies += "edu.ucar" % "grib" % "4.5.5"
libraryDependencies += "edu.ucar" % "netcdf4" % "4.5.5"
resolvers += Resolver.mavenLocal
