#/bin/sh

TARGET=./target/mahout-clustering-1.0-jar-with-dependencies.jar

hadoop jar $TARGET jp.opensquare.mahout.clustering.sample.MeigenToVectors;

hadoop jar $TARGET jp.opensquare.mahout.clustering.MeigenClustering
