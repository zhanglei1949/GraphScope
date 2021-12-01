# GRAPE JDK on GraphScope analytics

GRAPE JDK is a subproject under GraphScope, presenting an efficient java SDK for GraphScope analytical engine-GRAPE.
Powered By [Alibaba-FastFFI](https://github.com/alibaba/fastFFI), which is able to bridge the huge programming gap between Java and C++, Java PIE
SDK enables Java Programmers to acquire the following abilities

- **Ease of developing graph algorithms in Java**.

  PIE SDK mirrors the full-featured grape framework, including ```Fragment```, ```MessageManager```, etc.
  Armed with PIE SDK, a Java Programmer can develop algorithms in grape PIE model with no efforts.
- **Efficient execution for Java graph algorithms**.

  There are a bunch of graph computing platforms developed in Java, and providing programming interfaces
  in java. However, since Java, the language itself, lacks the ability to access low-level system resources
  and memories, these java frameworks are drastically outperformed when compared to C++ framework, grape.
  To provide efficient execution for java graph programs, simple JNI bridging is never enough. By
  leveraging the JNI acceleration provided by [```LLVM4JNI```](https://github.com/alibaba/fastFFI/tree/main/llvm4jni), PIE SDK substantially narrows the gap
  between java app and c++ app.
  As experiments shows, the minimum gap is around 1.5x.

- **Seamless integration with GraphScope**.

  To run a Java app developed with PIE SDK, the user just need to pack Java app into ```jar``` and
  submit in python client, as show in example. The input graph can be either property graph or
  projected graph in GraphScope, and the output can be redirected to client fs, vineyard just like
  normal GraphScope apps.
  
## Structure

- **grape-demo**
  
  Providing examples apps and [FFIMirrors](#user-defined-data-structure).
- **grape-runtime**
  
  Contains necessary static functions to create URL class loader, which is later
  used to load user jars.
  Annotation processor design for graphscope sdk, responsible for java and jni
  code generation, will be invoked by grape-engine, when ```ENABLE_JAVA_SDK```set to true.
- **grape-jdk**

  The core java sdk defines graph computing interfaces.

## Get grape-jdk

### Building from source

First you need **fastFFI** installed, see [alibaba/fastFFI](https://github.com/alibaba/fastFFI) for
installation guide.

```bash
git clone https://github.com/alibaba/GraphScope.git
cd analytical_engine/java/grape-jdk
mvn clean install
```

This will only install `grape-jdk` for you, if you are only interested in writing 
graph algorithms in java, that's enough for you :D.

To build the whole project, make sure there is one usable c++ compiler in your envirment
and both [`GraphScope-Analytical engine`](https://github.com/alibaba/GraphScope/tree/main/analytical_engine) 
and [`Vineyard`](https://github.com/v6d-io/v6d) is installed.

### From Maven Central repo

TODO

## Getting Started

- [Implement your own algorithm](https://graphscope.io/docs/analytics_engine.html#writing-your-own-algorithms-in-java)


# Documentation

Online JavaDoc is availabel at [GraphScope Docs](https://graphscope.io/docs/reference/gae_java/index.html).

You can also generate the documentation with in three different ways.
- use Intellij IDEA plugin: [Intellij IDEA-javadoc](https://www.jetbrains.com/help/idea/working-with-code-documentation.html) 
- Use Eclipse plugin: [Eclipse-javadoc](https://www.tutorialspoint.com/How-to-write-generate-and-use-Javadoc-in-Eclipse). 
- Generate javaDoc from cmd.
```bash
cd ${GRAPHSCOPE_REPO}/analytical_engine/java/grape-jdk
mvn javadoc::javadoc -Djavadoc.output.directory=${OUTPUT_DIR} -Djavadoc.output.destDir=${OUTPUT_DEST_DIR}
```

# Performance

Apart from the user-friendly interface, grape-jdk also provide user with high performance graph 
analytics experience. Please refer to [benchmark](performance.md) for the benchmark results.

# TODO
- Support more programming model
  - Giraph(Pregel)
- A test suite for verifying algorithm correctness, without GraphScope analytical engine.
- Documentation
- User-friendly error report





