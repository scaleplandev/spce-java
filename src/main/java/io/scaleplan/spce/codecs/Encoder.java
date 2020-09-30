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

package io.scaleplan.spce.codecs;

import io.scaleplan.spce.CloudEvent;
import org.jetbrains.annotations.NotNull;

import java.io.OutputStream;
import java.util.Collection;

public interface Encoder {
    @NotNull byte[] encode(final @NotNull CloudEvent event);

    void encode(@NotNull final CloudEvent event, @NotNull final OutputStream outputStream);

    @NotNull byte[] encode(@NotNull final Collection<CloudEvent> events);

    void encode(@NotNull final Collection<CloudEvent> events, @NotNull final OutputStream outputStream);
}
