/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.common.schema;

import org.apache.flink.cdc.common.annotation.PublicEvolving;
import org.apache.flink.cdc.common.types.DataType;

import javax.annotation.Nullable;

/** Representation of a physical column. */
@PublicEvolving
public class PhysicalColumn extends Column {

    private static final long serialVersionUID = 1L;

    public PhysicalColumn(String name, DataType type, @Nullable String comment) {
        super(name, type, comment);
    }

    public PhysicalColumn(
            String name, DataType type, @Nullable String comment, @Nullable String defaultValue) {
        super(name, type, comment, defaultValue);
    }

    @Override
    public boolean isPhysical() {
        return true;
    }

    @Override
    public Column copy(DataType newType) {
        return new PhysicalColumn(name, newType, comment, defaultValueExpression);
    }

    @Override
    public Column copy(String newName) {
        return new PhysicalColumn(newName, type, comment, defaultValueExpression);
    }
}
