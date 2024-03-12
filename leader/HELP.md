# Getting Started

### Reference Documentation
For further reference, please consider the following sections:

* [Official Apache Maven documentation](https://maven.apache.org/guides/index.html)
* [Spring Boot Maven Plugin Reference Guide](https://docs.spring.io/spring-boot/docs/3.2.3/maven-plugin/reference/html/)
* [Create an OCI image](https://docs.spring.io/spring-boot/docs/3.2.3/maven-plugin/reference/html/#build-image)
* [GraalVM Native Image Support](https://docs.spring.io/spring-boot/docs/3.2.3/reference/html/native-image.html#native-image)
* [Spring Integration AMQP Module Reference Guide](https://docs.spring.io/spring-integration/reference/html/amqp.html)
* [Spring Integration Test Module Reference Guide](https://docs.spring.io/spring-integration/reference/html/testing.html)
* [Spring for RabbitMQ](https://docs.spring.io/spring-boot/docs/3.2.3/reference/htmlsingle/index.html#messaging.amqp)
* [Docker Compose Support](https://docs.spring.io/spring-boot/docs/3.2.3/reference/htmlsingle/index.html#features.docker-compose)
* [Spring Batch](https://docs.spring.io/spring-boot/docs/3.2.3/reference/htmlsingle/index.html#howto.batch)
* [Spring Integration](https://docs.spring.io/spring-boot/docs/3.2.3/reference/htmlsingle/index.html#messaging.spring-integration)

### Guides
The following guides illustrate how to use some features concretely:

* [Messaging with RabbitMQ](https://spring.io/guides/gs/messaging-rabbitmq/)
* [Creating a Batch Service](https://spring.io/guides/gs/batch-processing/)
* [Integrating Data](https://spring.io/guides/gs/integration/)

### Additional Links
These additional references should also help you:

* [Configure AOT settings in Build Plugin](https://docs.spring.io/spring-boot/docs/3.2.3/maven-plugin/reference/htmlsingle/#aot)

### Docker Compose support
This project contains a Docker Compose file named `compose.yaml`.
In this file, the following services have been defined:

* postgres: [`postgres:latest`](https://hub.docker.com/_/postgres)
* rabbitmq: [`rabbitmq:latest`](https://hub.docker.com/_/rabbitmq)

Please review the tags of the used images and set them to the same as you're running in production.

## GraalVM Native Support

This project has been configured to let you generate either a lightweight container or a native executable.
It is also possible to run your tests in a native image.

### Lightweight Container with Cloud Native Buildpacks
If you're already familiar with Spring Boot container images support, this is the easiest way to get started.
Docker should be installed and configured on your machine prior to creating the image.

To create the image, run the following goal:

```
$ ./mvnw spring-boot:build-image -Pnative
```

Then, you can run the app like any other container:

```
$ docker run --rm leader:0.0.1-SNAPSHOT
```

### Executable with Native Build Tools
Use this option if you want to explore more options such as running your tests in a native image.
The GraalVM `native-image` compiler should be installed and configured on your machine.

NOTE: GraalVM 22.3+ is required.

To create the executable, run the following goal:

```
$ ./mvnw native:compile -Pnative
```

Then, you can run the app as follows:
```
$ target/leader
```

You can also run your existing tests suite in a native image.
This is an efficient way to validate the compatibility of your application.

To run your existing tests in a native image, run the following goal:

```
$ ./mvnw test -PnativeTest
```

