Rapids Shuffle Plugin
=====================

!! THIS IS AN ALPHA FEATURE: data corruption is possible, use with care !!

In order to build the Rapids Shuffle Plugin, you need jucx-1.8.0.jar. The current version we are using is `99c7a73cc`, but this should move more towards HEAD.

1. Clone the ucx repo:

```
git clone git@github.com:openucx/ucx.git
cd ucx
git checkout 99c7a73cc
```

2. Making sure you have `autotools` installed, run `autogen.sh` to generate the `configure` script,  
and then create a build directory inside `ucx`:

```
./autogen.sh
mkdir build
cd build
```

3. Configure the build. This only builds the cuda components:

```
../configure --prefix=[INSTALL PATH] -with-cuda=/usr/local/cuda --enable-fault-injection --with-java=/usr/lib/jvm/java-8-openjdk-amd64/ --enable-gtest --enable-examples --no-recursion
```

4. Make and make install

```
make -j10 && make install
```

5. JUCX should be in the `lib` directory inside of `[INSTALL PATH]`. You can `mvn install` this file like so:

```
mvn install:install-file -Dfile=[INSTALL PATH]/lib/jucx-1.8.0.jar -DgroupId=org.openucx -DartifactId=jucx -Dversion=1.8.0 -Dpackaging=jar
```
