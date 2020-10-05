# ScalePlan CloudEvents for Java

Unofficial Java implementation for [CloudEvents](https://cloudevents.io/) v1.0.
Check out the [CloudEvents spec](https://github.com/cloudevents/spec/blob/v1.0/spec.md).

## Features

* Implements CloudEvents 1.0 spec.
* JSON and JSON batch encoding/decoding. (spec: [CloudEvents JSON 1.0](https://github.com/cloudevents/spec/blob/v1.0/json-format.md))
* Avro encoding/decoding  (spec: [CloudEvents Avro 1.0](https://github.com/cloudevents/spec/blob/v1.0/avro-format.md))
* Experimental alternative Avro and Avro batch encoding/decoding.
* Simple API.
* Fast. See: https://github.com/yuce/cloudevents-java-benchmarks

## Install

`spce` is published on Bintray.

### Maven

Update your `~/.m2/settings.xml` with ScalePlan's Bintray repo: 

```xml
    <!-- ... -->
    <profiles>
        <profile>
            <repositories>
                <repository>
                    <snapshots>
                        <enabled>false</enabled>
                    </snapshots>
                    <id>bintray-scaleplan-maven</id>
                    <name>bintray</name>
                    <url>https://dl.bintray.com/scaleplan/maven</url>
                </repository>
            </repositories>
            <pluginRepositories>
                <pluginRepository>
                    <snapshots>
                        <enabled>false</enabled>
                    </snapshots>
                    <id>bintray-scaleplan-maven</id>
                    <name>bintray-plugins</name>
                    <url>https://dl.bintray.com/scaleplan/maven</url>
                </pluginRepository>
            </pluginRepositories>
            <id>bintray</id>
        </profile>
    </profiles>
    <!-- ... -->
```

Then, add the dependency to your `pom.xml`:
```xml
<dependency>
  <groupId>io.scaleplan</groupId>
  <artifactId>spce</artifactId>
  <version>0.3.3</version>
  <type>pom</type>
</dependency>
```

### Gradle

Update your `build.gradle` with ScalePlan's Bintray repo:

```groovy
repositories {
    maven {
        url  "https://dl.bintray.com/scaleplan/maven" 
    }
}
```

Then add the dependency in `build.gradle`:
```groovy
implementation 'io.scaleplan:spce:0.3.3'
```

## Usage

### Creating Events

All CloudEvent required attributes except `specversion` are really required, no defaults/auto-generation.

All attributes are validated against the CloudEvents 1.0 spec. 

Create a CloudEvent with required attributes:

```java
import io.scaleplan.spce.MutableCloudEvent;

// ..
// specversion defaults to 1.0

CloudEvent event = CloudEvent.builder()
   .setType("OximeterMeasured")
   .setSource("/user/123#")
   .setId("567")
   .build();
```


Create a CloudEvent with required and optional attributes and string data:

```java
// ..

CloudEvent event = CloudEvent.builder()
    // ...
    .setTime("2020-07-13T09:15:12Z")
    .setDataContentType("application/json")
    .setData("{\"user_id\": \"bc1459c5-378d-4835-b4b6-a2c7d9ca75e3\"}")
    .setDataSchema("http://particlemetrics.com/sample-schema#")
    .setSubject("SampleSubject")
    // ...
```

Binary data is supported as well:
```java
    // ...
    .setData(new byte[]{1, 2, 3, 4})
    // ...
```

The data attribute is copied by default, you can use `.setDataUnsafe` to wrap the array.

`.setTime` expects a String or UNIX timestamp in milliseconds. You can use the `.setTimeNow` shortcut to use the current time:

```java
.setTimeNow(); // equivalent to: Instant.ofEpochMilli(System.currentTimeMillis()).toString()
``` 

Create a CloudEvent with extension attributes:

```java
CloudEvent event = CloudEvent.builder()
    // ...
    .put("exensionattr1key", "value1")
    .put("exensionattr2key", "value2")
    // ...
```

`put` throws an exception if the attribute name is one of the required or optional attributes.

You can access the required and optional attributes using `getXXX`:

```java
assertEquals("OximeterMeasured", event.getType());
assertEquals("application/json", event.getDataContentType());
```

Access extension attributes using `getAttributeXXX` methods:

* `Object getAttribute(String name)`
* `String getStringAttribute(String name)`
* `Integer getIntAttribute(String name)`
* `Boolean getBoolAttribute(String name)`

`CloudEvent` is a read-only interface, so the builder creates a read-only object.
```java
CloudEvent event = CloudEvent.builder()
    // ...
    // .build();
event.setType("other-type");  // NO-NO
```

Cast a `CloudEvent` to `MutableCloudEvent` to be able to modify it:
```java
MutableCloudEvent mutEvent = (MutableEvent)event;
event.setType("other-type");  // That's OK.
```

### Encoding/Decoding Events in JSON

Encode an event in JSON:

```java
import io.scaleplan.spce.codecs.Json;

// ...
byte[] encodedEvent = Json.encode(event);
// ...
```

Note that null/unset attributes won't be encoded.

Encode a batch of events in JSON:

```java
List<CloudEvent> events = Arrays.asList(event1, event2);
byte[] encodedEvents = Json.encode(events);
```

Decode an event in JSON:

```java
CloudEvent decodedEvent = Json.decode(encodedEvent);
```

Decode a batch of events in JSON:

```java
List<CloudEvent> decodedEvents = Json.decodeBatch(encodedEvents)
```

### Encoding/Decoding Events in Avro

Uses the official [Avro schema](cloudevents_spec.avsc). 

Encode an event in Avro:
```java
import io.scaleplan.spce.codecs.Avro;

// ...
byte[] encodedEvent = Avro.encode(event);
// ...
```

Decode an event in Avro:
```java
CloudEvent decodedEvent = Avro.decode(encodedEvent);
```

### Encoding/Decoding Events in Avro Spce (Experimental)

This library includes an alternative Avro schema for CloudEvents (`AvroSpce`) which offers smaller binaries and improved encoding/decoding speed.

*WARNING*: The binaries produced by the alternative schema is NOT compatible with the official CloudEvents schema!

Encode an event in Avro Spce:
```java
import io.scaleplan.spce.codecs.AvroSpce;

// ...
byte[] encodedEvent = AvroSpce.encode(event);
// ...
```

Encode a batch of events in Avro Spce:

```java
List<CloudEvent> events = Arrays.asList(event1, event2);
byte[] encodedEvents = AvcoSpce.encode(events);
```

Decode an event in Avro Spce:
```java
CloudEvent decodedEvent = AvroSpce.decode(encodedEvent);
```

Decode a batch of events in Avro Spce:

```java
List<CloudEvent> decodedEvents = AvroSpce.decodeBatch(encodedEvents)
```

## License

(c) 2020 Scale Plan Yazılım A.Ş. https://scaleplan.io

Licensed under [Apache 2.0](LICENSE). See the [LICENSE](LICENSE).

    Copyright 2020 Scale Plan Yazılım A.Ş.
    
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at
    
        http://www.apache.org/licenses/LICENSE-2.0
    
    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
