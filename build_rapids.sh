export URM_URL=https://urm.nvidia.com:443/artifactory/sw-spark-maven
mvn clean package -Dbuildver=330 -DskipTests -s jenkins/settings.xml
#mvn -Dbuilder=330 verify # -DskipTests -s jenkins/settings.xml
