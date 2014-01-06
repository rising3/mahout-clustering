rm -rf meigen-wakati
rm -rf meigen-seqfiles
rm -rf meigen-vectors

mkdir -p meigen-wakati

pushd .
cd meigen
for file in `find . -type f -name "*.txt"`
do
  mecab $file -o ../meigen-wakati/$file -O wakati
done
popd

mahout seqdirectory -i meigen-wakati -o meigen-seqfiles

mahout seqdumper -i meigen-seqfiles

mahout seq2sparse \
  -i meigen-seqfiles \
  -o meigen-vectors \
  -a org.apache.lucene.analysis.core.WhitespaceAnalyzer \
  -chunk 200 -wt tfidf -s 5 -md 3 -x 90 -ng 2 -ml 50 -seq

