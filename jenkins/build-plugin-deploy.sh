#!/bin/bash
##
#
# Script to build rapids-plugin jar files.
# Source tree is supposed to be ready by Jenkins before starting this script.
#
###
set -e

mvn clean package --settings=settings.xml deploy -Pgpuwa
