rm -rf meigen-canopy
rm -rf meigen-output

mahout canopy \
  -i meigen-vectors/tfidf-vectors \
  -o meigen-canopy \
  -t1 0.7 -t2 0.5 \
  -dm org.apache.mahout.common.distance.CosineDistanceMeasure

mahout kmeans \
  -i meigen-vectors/tfidf-vectors \
  -c meigen-canopy/clusters-0-final/part-r-00000 \
  -o meigen-output \
  --maxIter 50 -cl \
  -dm org.apache.mahout.common.distance.CosineDistanceMeasure

mahout clusterdump \
   -d meigen-vectors/dictionary.file-0 \
   -dt sequencefile \
   -i meigen-output/clusters-3-final \
   -o dump \
   -p meigen-output/clusteredPoints
