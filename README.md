# ScalePlan CloudEvents for Java

Unofficial Python implementation for [CloudEvents](https://cloudevents.io/) v1.0.
Check out the [CloudEvents spec](https://github.com/cloudevents/spec/blob/v1.0/spec.md).

## Features

* Implements CloudEvents 1.0 spec.
* JSON and JSON batch encoding/decoding.
* Simple API.
* Fast. See: https://github.com/scaleplandev/cloudevents-benchmarks

## Install

### Maven

### Gradle

## Usage

### Creating Events

Create a CloudEvent with required attributes:

```java
import io.scaleplan.spce.MutableCloudEvent;

// ..

String type = "OximeterMeasured";
String source = "oximeter/123";
String id = "1000"  
MutableCloudEvent event = MutableCloudEvent.create(type, source, id);
```


