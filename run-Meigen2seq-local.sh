#/bin/sh

rm -rf meigen-wakati
rm -rf meigen-seqfiles

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

