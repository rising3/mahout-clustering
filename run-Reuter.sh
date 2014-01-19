#/bin/sh

TARGET=./target/mahout-clustering-1.0-jar-with-dependencies.jar

hadoop jar $TARGET jp.opensquare.mahout.sample.ReuterVectors

hadoop jar $TARGET jp.opensquare.mahout.clustering.ReuterClustering
