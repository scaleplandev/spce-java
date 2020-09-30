// Copyright 2020 Scale Plan Yazılım A.Ş.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import io.scaleplan.spce.CloudEvent;
import io.scaleplan.spce.MutableCloudEvent;
import io.scaleplan.spce.ValidationException;
import io.scaleplan.spce.impl.MutableCloudEventImpl;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class EventValidationTest {
    @Test
    public void testValidEvent() {
        CloudEvent event = CloudEvent.builder()
                .setType("OximeterMeasured")
                .setSource("/user/123")
                .setId("2")
                .build();
        ((MutableCloudEventImpl) event).validate();
    }

    @Test
    public void testInvalidEventMissingSpec() {
        MutableCloudEvent event = MutableCloudEventImpl.create()
                .setType("OximeterMeasured")
                .setSource("/user/123")
                .setId("10");
        assertThrows(ValidationException.class, ((MutableCloudEventImpl) event)::validate);
    }

    @Test
    public void testInvalidEventMissingType() {
        MutableCloudEvent event = MutableCloudEventImpl.create()
                .setSpecVersion("OximeterMeasured")
                .setSource("/user/123")
                .setId("10");
        assertThrows(ValidationException.class, ((MutableCloudEventImpl) event)::validate);
    }

    @Test
    public void testInvalidEventMissingSource() {
        MutableCloudEvent event = MutableCloudEventImpl.create()
                .setSpecVersion("OximeterMeasured")
                .setType("OximeterMeasured")
                .setId("10");
        assertThrows(ValidationException.class, ((MutableCloudEventImpl) event)::validate);
    }

    @Test
    public void testInvalidEventMissingId() {
        MutableCloudEvent event = MutableCloudEventImpl.create()
                .setSpecVersion("OximeterMeasured")
                .setType("OximeterMeasured")
                .setSource("/user/123");
        assertThrows(ValidationException.class, ((MutableCloudEventImpl) event)::validate);
    }
}
