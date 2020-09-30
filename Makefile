.PHONY: build benchmark clean release test doc

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
