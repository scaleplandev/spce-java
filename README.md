# cloudevents-java

## Usage

### Creating a Stand-alone Package Without JDK Requirement

Creates a Zip archive with compiled project files, which **doesn't** require the JDK to be present.

JDK 11 or higher is required to create the package.

* Run `make package-standalone` to create the Zip archive *build/PROJECT_NAME-PROJECT_VERSION.zip*
* When the archive is unpackaged, the `bin` directory contains the `PROJECT_NAME` executable which starts the project.

### Creating a Jar Package

Creates a Jar file with all dependencies. Running the Jar requires a JDK or to be present. A minimal JRE 8 for Linux can be found at: https://scaleplan.net/jre 

JDK 8 or higher is required to create the Jar.

* Run `make package` to create the Jar file `build/libs/PROJECT_NAME-PROJECT_VERSION-all.jar`
* The Jar can be run using: `java -jar PROJECT_NAME-PROJECT_VERSION-all.jar` (assuming the Jar is in the current directory).

### Creating a Distribution Package 

Creates an archive with the project and startup scripts. This way of creating a package is almost equivalent to the Jar package explained above but has shell scripts to run the Jar.

* Run `make package-dist` to create the archive `build/distributions/PROJECT_NAME-shadow-PROJECT_VERSION-SNAPSHOT.zip
* When the archive is unpackaged, the `bin` directory contains the `PROJECT_NAME-PROJECT_VERSION` executable which starts the project.

