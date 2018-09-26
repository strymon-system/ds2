How to apply the patch
---------------
1. Download Apache Flink 1.4.1 from [here](https://archive.apache.org/dist/flink/flink-1.4.1/flink-1.4.1-src.tgz).
2. Extract the folder contents. The patch expects a directory structure of the form `/workspace/flink-1.4.1-instrumented/flink-1.4.1/`
3. Check and apply the patch using `git apply --check ds2.patch` and `git am --signoff < ds2.patch`
4. Build the source code running `mvn clean package -DskipTests` inside the flink home directory.
5. Create a folder names `rates/` in the flink home directory.
6. Configure and run your flink application.
