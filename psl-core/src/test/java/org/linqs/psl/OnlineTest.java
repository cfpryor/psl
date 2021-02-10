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
package org.linqs.psl;

import org.linqs.psl.application.inference.online.OnlineClient;
import org.linqs.psl.application.inference.online.messages.actions.OnlineAction;
import org.linqs.psl.application.inference.online.messages.responses.OnlineResponse;
import org.linqs.psl.application.inference.online.messages.responses.QueryAtomResponse;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.Assert.assertEquals;

/**
 * Utilities for Online PSL Inference Tests.
 */
public class OnlineTest {
    public static List<OnlineResponse> clientSession(OnlineAction onlineAction) {
        BlockingQueue<OnlineAction> onlineActions = new LinkedBlockingQueue<OnlineAction>();
        onlineActions.add(onlineAction);

        return clientSession(onlineActions);
    }

    public static List<OnlineResponse> clientSession(BlockingQueue<OnlineAction> onlineActions) {
        OnlineClient onlineClient = null;
        List<OnlineResponse> sessionOutput = new ArrayList<OnlineResponse>();

        onlineClient = new OnlineClient(new PrintStream(new ByteArrayOutputStream()), onlineActions, sessionOutput,
                new CountDownLatch(0));
        Thread onlineClientThread = new Thread(onlineClient);
        onlineClientThread.start();

        try {
            onlineClientThread.join();
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }

        return sessionOutput;
    }

    public static void assertAtomValues(BlockingQueue<OnlineAction> commands, double[] values) {
        List<OnlineResponse> onlineResponses = null;

        onlineResponses = clientSession(commands);

        int i = 0;
        for (OnlineResponse onlineResponse : onlineResponses) {
            if (onlineResponse instanceof QueryAtomResponse) {
                assertEquals(values[i], ((QueryAtomResponse)onlineResponse).getAtomValue(), 0.1);
                i++;
            }
        }

        assertEquals(i, values.length);
    }
}
