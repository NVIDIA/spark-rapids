#!/bin/bash

# Copyright (c) 2021, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# This script generates the commits that went in Apache Spark for audit.
# Audit is required to evaluate if the code needs to be updated based
# on new commits merged in Apache Spark. This currently audits changes for
# Spark-3.2 (master branch).
# Arguments:
#   lastcommit - File which contains the latest commit hash when this script ran last.
#   basebranch - branch in Apache Spark for which commits needs to be audited.
#                Currently it's master as Spark-3.2 branch is not cut yet.
#   tag        - tag until which the commits are audited 


set -ex
ABSOLUTE_PATH=$(cd $(dirname $0) && pwd)
lastcommit=""
basebranch="master"
tag="v3.1.1-rc3"
REF=${REF:-"main"}
REF=main
while getopts v:b:t: flag
do
  case "${flag}" in
      v) lastcommit=${OPTARG};;
      b) basebranch=${OPTARG};;
      t) tag=${OPTARG};;
  esac
done

SPARK_TREE="$WORKSPACE/spark"
if [ -e ${SPARK_TREE} ]; then
 rm -rf $SPARK_TREE 
fi
git clone https://github.com/apache/spark.git $SPARK_TREE

if [ -f "$lastcommit" ]; then
    cd ${SPARK_TREE}
    latestcommit=`cat ${lastcommit}`
    git checkout $basebranch
    git log --oneline HEAD...$latestcommit -- sql/core/src/main sql/catalyst/src/main | tee ${COMMIT_DIFF_LOG}
    git log HEAD -n 1 --pretty="%h" > ${lastcommit}

    cd $WORKSPACE
    set +ex
    COMMIT_UPDATE=`git diff ${lastcommit}`
    set -ex
    if [ -n "$COMMIT_UPDATE" ]; then
        git config --global user.name blossom
        git config --global user.email blossom@nvidia.com
        git add ${lastcommit}
        git commit -m "Update latest commit-id for org.apache.spark branch ${basebranch}"
        git push origin HEAD:$REF
    else
        echo "No commit update"
    fi
else
    ## Below sequence of commands were used to get the initial list of commits to audit branch-3.2-SNAPSHOT(which is currently `master` branch)
    ## It filters out all the commits that were audited until 3.1.1-rc3.
    ## There wasn't easy way to get the list of commits to audit for branch-3.2-SNAPSHOT.
    ## Spark release works in this way -  Once the release branch is cut, PR's are merged into master and then cherry-picked to release branches.
    ## This causes different commit ids for the same PR in different branches(master & release branch).
    ## We need to find the common parent before branch-3.1 was cut. In this case commit id 990bee9c58e is the one.
    ## So we get all commits from master and branch-3.1 until the common parent commit and then filter it based on commit header message i.e
    ## if the commit header is same, it means it is cherry-picked to branch-3.1(implying that commit is already audited).
    echo "file $lastcommit not found"
    cd ${SPARK_TREE}

    ## Get all the commits from TOT tagv3.1.1-rc3 to 990bee9c58e
    git checkout $tag
    git log --oneline HEAD...990bee9c58e -- sql/core/src/main sql/catalyst/src/main  > b3.1.1.log

    ## Get all the commits from TOT master to 990bee9c58e
    git checkout $basebranch
    git log --oneline HEAD...990bee9c58e -- sql/core/src/main sql/catalyst/src/main  > b3.2.log

    ## Below steps filter commit header messages, sorts and saves only uniq commits that needs to be audited in commits.to.audit.3.2 file
    cat b3.1.1.log | awk '{$1 = "";print $0}' > b3.1.1.filter.log
    cat b3.2.log | awk '{$1 = "";print $0}' > b3.2.filter.log
    cat b3.2.filter.log b3.1.1.filter.log | sort | uniq -c | sort  | awk '/^[[:space:]]*1/{$1 = "";print $0}' > uniqcommits.log
    cat b3.1.1.filter.log | sort > b3.1.1.filter.sorted.log
    cat b3.2.filter.log | sort > b3.2.filter.sorted.log
    cat uniqcommits.log | sort > uniqcommits.sorted.log
    comm -12 b3.1.1.filter.sorted.log uniqcommits.sorted.log | wc -l
    comm -12 b3.2.filter.sorted.log uniqcommits.sorted.log > commits.to.audit.3.2
    sed -i 's/\[/\\[/g' commits.to.audit.3.2
    sed -i 's/\]/\\]/g' commits.to.audit.3.2

    filename=commits.to.audit.3.2
    while read -r line; do
      echo "1"
      git log --grep="$line" --pretty="%h %s" >> ${COMMIT_DIFF_LOG}
    done < $filename
    git log HEAD -n 1 --pretty="%h" > $lastcommit
fi
cd ${ABSOLUTE_PATH}/../ 
. scripts/prioritize-commits.sh
