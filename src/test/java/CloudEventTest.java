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

import static org.junit.jupiter.api.Assertions.*;

public class CloudEventTest {
    @Test
    public void testRequiredAttributes() {
        CloudEvent event = Common.sampleEventWithRequiredAttributes();
        Common.checkEvent(event,
                null,
                "id", "567",
                "specversion", "1.0",
                "source", "/user/123#",
                "type", "OximeterMeasured"
        );
        assertEquals("1.0", event.getSpecVersion());
        assertEquals("OximeterMeasured", event.getType());
        assertEquals("/user/123#", event.getSource());
        assertEquals("567", event.getId());
    }

    @Test
    public void testOptionalAttributes() {
        CloudEvent baseEvent = Common.sampleEventWithRequiredAttributes();
        // MutableEventImpl do
        CloudEvent event = ((MutableCloudEvent) baseEvent)
                .setTime("2020-07-13T09:15:12Z")
                .setDataContentType("application/json")
                .setData(Common.sampleStringData)
                .setDataSchema("https://particlemetrics.com/oximeter-schema#")
                .setSubject("SampleSubject");
        Common.checkEvent(event,
                Common.sampleStringData.getBytes(),
                "id", "567",
                "specversion", "1.0",
                "source", "/user/123#",
                "type", "OximeterMeasured",
                "time", "2020-07-13T09:15:12Z",
                "datacontenttype", "application/json",
                "dataschema", "https://particlemetrics.com/oximeter-schema#",
                "subject", "SampleSubject"
        );
        assertEquals("1.0", event.getSpecVersion());
        assertEquals("OximeterMeasured", event.getType());
        assertEquals("/user/123#", event.getSource());
        assertEquals("567", event.getId());
        assertEquals("2020-07-13T09:15:12Z", event.getTime());
        assertEquals("application/json", event.getDataContentType());
        assertArrayEquals(Common.sampleStringData.getBytes(), event.getData());
        assertEquals("https://particlemetrics.com/oximeter-schema#", event.getDataSchema());
        assertEquals("SampleSubject", event.getSubject());
    }

    @Test
    public void testExtendedAttributes() {
        CloudEvent baseEvent = Common.sampleEventWithRequiredAttributes();
        // MutableEventImpl do
        CloudEvent event = ((MutableCloudEvent) baseEvent)
                .put("compmstring", "string-value")
                .put("compmint", 42)
                .put("compmbool", true);
        assertEquals("1.0", event.getSpecVersion());
        assertEquals("OximeterMeasured", event.getType());
        assertEquals("/user/123#", event.getSource());
        assertEquals("567", event.getId());
        assertEquals("string-value", event.getAttribute("compmstring"));
        assertEquals("string-value", event.getStringAttribute("compmstring"));
        assertEquals(42, event.getIntAttribute("compmint"));
        assertEquals(true, event.getBoolAttribute("compmbool"));
    }

    @Test
    public void testBinaryData() {
        CloudEvent event = MutableCloudEventImpl.create()
                .setData("foobar");
        assertFalse(event.hasBinaryData());
    }

    @Test
    public void testStringData() {
        CloudEvent event = MutableCloudEventImpl.create()
                .setData("foobar".getBytes());
        assertTrue(event.hasBinaryData());
    }

    @Test
    public void testDeleteAttribute() {
        MutableCloudEvent event = MutableCloudEventImpl.create()
                .put("foobar", "zoo");
        assertEquals("zoo", event.getAttribute("foobar"));
        event.remove("foobar");
        assertNull(event.getAttribute("foobar"));
    }

    @Test
    public void testCannotDeleteRequiredAttribute() {
        MutableCloudEvent event = MutableCloudEventImpl.create().setType("MyEvent");
        assertEquals("MyEvent", event.getType());
        assertThrows(IllegalArgumentException.class,
                () -> event.remove("type"));
    }

    @Test
    public void testInvalidTimeThrows() {
        CloudEvent.Builder builder = CloudEvent.builder();
        assertThrows(ValidationException.class, () -> builder.setTime("invalid"));
    }

    @Test
    public void testUnsafeAndSafe() {
        CloudEvent event = CloudEvent.builder()
                .setType("OximeterMeasured")
                .setSource("/user/123#")
                .setId("567")
                .setTime("2020-07-13T09:15:12Z")
                .setDataContentType("application/json")
                .setData(Common.sampleStringData)
                .setDataSchema("http://json-schema.org/draft-07/schema#")
                .setSubject("SampleSubject")
                .put("compmstring", "string-value")
                .build();

    }

}
