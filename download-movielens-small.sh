#!/usr/bin/env bash
wget http://files.grouplens.org/datasets/movielens/ml-latest-small.zip &&
unzip ml-latest-small.zip &&
mv ml-latest-small/ratings.csv src/main/resources/small/ratings.csv &&
mv ml-latest-small/movies.csv src/main/resources/small/movies.csv &&
rm -rf ml-latest-small
rm -rf ml-latest-small.zip