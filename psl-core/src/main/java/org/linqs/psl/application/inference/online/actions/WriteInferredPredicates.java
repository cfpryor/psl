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
package org.linqs.psl.application.inference.online.actions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WriteInferredPredicates extends OnlineAction{

    private String outputDirectoryPath;
    private static final Logger log = LoggerFactory.getLogger(WriteInferredPredicates.class);

    public WriteInferredPredicates() {
        outputDirectoryPath = null;
    }

    @Override
    public String getName() {
        return "WriteInferredPredicates";
    }

    public String getOutputDirectoryPath() {
        return outputDirectoryPath;
    }

    @Override
    public void initAction(String[] tokenized_command) throws IllegalArgumentException {
        // Format: WriteInferredPredicates outputDirectoryPath(optional)
        for (int i = 1; i < tokenized_command.length; i++) {
            if (i == 1) {
                // outputDirectoryPath Field:
                outputDirectoryPath = tokenized_command[i];
            } else {
                throw new IllegalArgumentException("Too many arguments provided for Action: WriteInferredPredicates");
            }
        }
    }
}
