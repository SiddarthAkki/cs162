#!/bin/sh
sudo apt-get update
sudo apt-get install ant openjdk-7-jdk
echo "export JAVA_HOME=/usr/lib/jvm/java-7-openjdk-amd64/jre" >> ~/.zshrc
source ~/.zshrc
