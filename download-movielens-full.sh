#!/usr/bin/env bash
wget http://files.grouplens.org/datasets/movielens/ml-latest.zip &&
unzip ml-latest.zip &&
mv ml-latest/ratings.csv src/main/resources/full/ratings.csv &&
mv ml-latest/movies.csv src/main/resources/full/movies.csv &&
rm -rf ml-latest
rm -rf ml-latest.zip