name := "spark-streaming-gnip"

version := "1.0"

scalaVersion := "2.11.6"

organization := "com.knoldus"

libraryDependencies ++= Seq(
                      "org.apache.spark"	%%	"spark-core"	    %	"1.4.0",
                      "org.apache.spark"	%%	"spark-streaming"	% 	"1.4.0"
		       )		       