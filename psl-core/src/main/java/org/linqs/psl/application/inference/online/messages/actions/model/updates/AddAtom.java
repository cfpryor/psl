/*
 * This file is part of the PSL software.
 * Copyright 2011-2015 University of Maryland
 * Copyright 2013-2020 The Regents of the University of California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.linqs.psl.application.inference.online.messages.actions.model.updates;

import org.linqs.psl.application.inference.online.messages.actions.OnlineAction;
import org.linqs.psl.model.predicate.StandardPredicate;
import org.linqs.psl.model.term.Constant;
import org.linqs.psl.util.RandUtils;
import org.linqs.psl.util.StringUtils;

/**
 * Add a new atom to the model.
 * String format: ADD <READ/WRITE> <predicate> <args> ... [value]
 */
public class AddAtom extends OnlineAction {
    private StandardPredicate predicate;
    private String partition;
    private Constant[] arguments;
    private float value;

    public AddAtom(String partition, StandardPredicate predicate, Constant[] arguments, float value) {
        super();
        this.predicate = predicate;
        this.arguments = arguments;
        this.partition = partition.toUpperCase();
        this.value = value;
    }

    public StandardPredicate getPredicate() {
        return predicate;
    }

    public String getPartitionName() {
        return partition;
    }

    public float getValue() {
        if (value == -1.0f) {
            // Default value when a value is not provided.
            return RandUtils.nextFloat();
        } else {
            return value;
        }
    }

    public Constant[] getArguments() {
        return arguments;
    }

    @Override
    public String toString() {
        if (value == -1.0f) {
            return String.format(
                    "ADD\t%s\t%s\t%s",
                    partition,
                    predicate.getName(),
                    StringUtils.join("\t", arguments).replace("'", ""));
        } else {
            return String.format(
                    "ADD\t%s\t%s\t%s\t%.2f",
                    partition,
                    predicate.getName(),
                    StringUtils.join("\t", arguments).replace("'", ""),
                    value);
        }
    }
}
