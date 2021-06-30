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
package org.linqs.psl.cli;

import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.linqs.psl.application.inference.online.messages.OnlineMessage;
import org.linqs.psl.application.inference.online.messages.actions.controls.Exit;
import org.linqs.psl.application.inference.online.messages.responses.OnlineResponse;
import org.linqs.psl.parser.OnlineActionLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.InputMismatchException;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * A client that takes input on stdin and passes it to the online host specified in configuration.
 */
public class OnlineClient {
    private static final Logger log = LoggerFactory.getLogger(OnlineClient.class);

    private OnlineClient() {}

    public static List<OnlineResponse> run(InputStream in, PrintStream out) {
        List<OnlineResponse> serverResponses = new ArrayList<OnlineResponse>();
        CountDownLatch modelRegistrationLatch = new CountDownLatch(1);

        try (BufferedReader commandReader = new BufferedReader(new InputStreamReader(in))) {
            String userInput = null;
            OnlineMessage onlineAction = null;
            BlockingQueue<OnlineMessage> onlineActions = new LinkedBlockingQueue<OnlineMessage>();

            // Initialize onlineClient with shared data structures.
            org.linqs.psl.application.inference.online.OnlineClient onlineClient =
                    new org.linqs.psl.application.inference.online.OnlineClient(out, onlineActions,
                            serverResponses, modelRegistrationLatch);

            // Run onlineClient thread for client session.
            Thread onlineClientThread = new Thread(onlineClient);
            onlineClientThread.start();

            // Wait for countdown indicating model registered.
            modelRegistrationLatch.await();

            // Read and parse userInput to create actions to send to server.
            while (!(onlineAction instanceof Exit)) {
                try {
                    // Read next command.
                    userInput = commandReader.readLine();
                    if (userInput == null) {
                        break;
                    }

                    // Parse command.
                    if (userInput.equals("")) {
                        continue;
                    }
                    onlineAction = OnlineActionLoader.loadAction(userInput);
                    onlineActions.add(onlineAction);
                } catch (ParseCancellationException | InputMismatchException ex) {
                    log.error(String.format("Error parsing command: [%s].", userInput));
                    log.error(ex.getMessage());
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            }

            // Wait for serverConnectionThread.
            onlineClientThread.join();
        } catch(IOException ex) {
            throw new RuntimeException(ex);
        } catch (InterruptedException ex) {
            // Ignore.
        }

        return serverResponses;
    }
}

