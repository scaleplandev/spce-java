.PHONY: build benchmark clean release test doc

AVRO_TOOLS_PATH ?= ./avro-tools-1.10.0.jar

build:
	./gradlew build

benchmark:
	./gradlew --no-daemon jmh

clean:
	./gradlew clean

release: clean build
	./gradlew publish

test:
	./gradlew check

doc:
	./gradlew javadoc

generate:
	java -jar $(AVRO_TOOLS_PATH) compile schema ./etc/cloudevents_spec.avsc ./src/main/java

generate-alt:
	java -jar $(AVRO_TOOLS_PATH) compile schema -string ./etc/cloudevents_spec_alt.avsc ./src/main/java
