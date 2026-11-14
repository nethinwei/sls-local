#!/bin/sh
# 由 SqlBase.g4 重新生成 ../parser。需要 Docker（ANTLR 工具是 Java 程序，生成的 Go 代码已提交，平时编译不需要）。
# SqlBase.g4 取自 prestodb/presto 0.290（presto-parser/src/main/antlr4/com/facebook/presto/sql/parser/SqlBase.g4，
# Apache-2.0），未做修改。ANTLR 版本须与 go.mod 里的 github.com/antlr4-go/antlr/v4 一致。
set -eu
cd "$(dirname "$0")"
JAR=antlr-4.13.1-complete.jar
SHA1=e59266b0351e7543f19834614aa76551023ab4cc
if [ ! -f "$JAR" ]; then
  curl -fsSL -o "$JAR" "https://repo1.maven.org/maven2/org/antlr/antlr4/4.13.1/antlr4-4.13.1-complete.jar"
fi
echo "$SHA1  $JAR" | shasum -c -
rm -rf ../parser && mkdir ../parser
docker run --rm -v "$PWD/..":/work -w /work/grammar \
  eclipse-temurin:21-jre@sha256:18bb4a91a3dea3f03ee31d3755af34dfb40f904fc77ab56cdc2f092e32fff9c2 \
  java -jar "$JAR" -Dlanguage=Go -package parser -visitor -no-listener -Xexact-output-dir -o ../parser SqlBase.g4
rm -f ../parser/*.interp ../parser/*.tokens
# 语法里 predicate[ParserRuleContext value] 用的是 Java 类型名，Go 目标需要包名前缀
perl -pi -e 's/(?<![\w.])ParserRuleContext\b/antlr.ParserRuleContext/g' ../parser/*.go
gofmt -w ../parser
