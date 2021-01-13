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
package org.linqs.psl.application.inference.online;

import org.linqs.psl.application.inference.online.messages.actions.OnlineAction;
import org.linqs.psl.application.inference.online.messages.actions.controls.Exit;
import org.linqs.psl.application.inference.online.messages.actions.controls.Stop;
import org.linqs.psl.application.inference.online.messages.responses.ModelInformation;
import org.linqs.psl.application.inference.online.messages.responses.OnlineResponse;
import org.linqs.psl.config.Options;

import org.linqs.psl.model.predicate.Predicate;
import org.linqs.psl.model.predicate.StandardPredicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.PrintStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 * A client that takes input on stdin and passes it to the online host specified in configuration.
 */
public class OnlineClient implements Runnable {
    private Logger log = LoggerFactory.getLogger(OnlineClient.class);

    private PrintStream out;
    private List<OnlineResponse> serverResponses;
    private BlockingQueue<OnlineAction> actionQueue;
    private String hostname;
    private int port;

    public OnlineClient(PrintStream out, BlockingQueue<OnlineAction> actionQueue, List<OnlineResponse> serverResponses) {
        this.out = out;
        this.serverResponses = serverResponses;
        this.actionQueue = actionQueue;
        this.hostname = Options.ONLINE_HOST.getString();
        this.port = Options.ONLINE_PORT_NUMBER.getInt();
    }

    public void run() {
        try (
                Socket server = new Socket(hostname, port);
                ObjectOutputStream socketOutputStream = new ObjectOutputStream(server.getOutputStream());
                ObjectInputStream socketInputStream = new ObjectInputStream(server.getInputStream())) {
            OnlineAction onlineAction = null;

            // Read and register serverModel.
            registerServerModel(socketInputStream);

            // Startup serverConnectionThread for reading server responses.
            ServerConnectionThread serverConnectionThread = new ServerConnectionThread(server, socketInputStream, out, serverResponses);
            serverConnectionThread.start();

            // Deque actions and send to server.
            while (!(onlineAction instanceof Exit || onlineAction instanceof Stop)) {
                try {
                    // Dequeue next action.
                    try {
                        onlineAction = actionQueue.take();
                    } catch (InterruptedException ex) {
                        log.warn("Interrupted while taking an online action from the queue.", ex);
                        continue;
                    }

                    socketOutputStream.writeObject(onlineAction);
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            }

            // Wait for serverConnectionThread.
            serverConnectionThread.join();
        } catch(IOException ex) {
            throw new RuntimeException(ex);
        } catch (InterruptedException ex) {
            log.error("Client session interrupted");
        }
    }

    public void initLocalModel() {
        try (
                Socket server = new Socket(hostname, port);
                ObjectOutputStream socketOutputStream = new ObjectOutputStream(server.getOutputStream());
                ObjectInputStream socketInputStream = new ObjectInputStream(server.getInputStream())) {

            // Read and register serverModel.
            registerServerModel(socketInputStream);

            // Startup serverConnectionThread for reading server responses.
            ServerConnectionThread serverConnectionThread = new ServerConnectionThread(server, socketInputStream, out, serverResponses);
            serverConnectionThread.start();

            // Cleanly exit server connection.
            socketOutputStream.writeObject(new Exit());

            // Wait for serverConnectionThread.
            serverConnectionThread.join();
        } catch(IOException ex) {
            throw new RuntimeException(ex);
        } catch (InterruptedException ex) {
            log.error("Client session interrupted");
        }
    }

    private void registerServerModel(ObjectInputStream socketInputStream) {
        // Get model information from server.
        ModelInformation modelInformation = null;
        try {
            modelInformation = (ModelInformation)socketInputStream.readObject();
        } catch (IOException | ClassNotFoundException ex) {
            throw new RuntimeException(ex);
        }

        // Register model predicates.
        for (StandardPredicate predicate : modelInformation.predicates.values()) {
            // Todo (Charles): We will likely want to replace if predicate already existed.
            Predicate.addPredicateIfAbsent(predicate);
            log.trace("Registered predicate: " + StandardPredicate.get(predicate.getName()).toString() +
                    " Client Hash: " + StandardPredicate.get(predicate.getName()).hashCode() +
                    " Server Hash: " + predicate.hashCode());
        }
    }

    /**
     * Private class for reading OnlineResponse objects from server.
     */
    private static class ServerConnectionThread extends Thread {
        private Logger log = LoggerFactory.getLogger(ServerConnectionThread.class);

        private ObjectInputStream inputStream;
        private PrintStream out;
        private Socket socket;
        private List<OnlineResponse> serverResponses;

        public ServerConnectionThread(Socket socket, ObjectInputStream inputStream, PrintStream out, List<OnlineResponse> serverResponses) {
            this.socket = socket;
            this.inputStream = inputStream;
            this.out = out;
            this.serverResponses = serverResponses;
        }

        @Override
        public void run() {
            OnlineResponse response = null;

            while (socket.isConnected() && !isInterrupted()) {
                try {
                    response = (OnlineResponse)inputStream.readObject();
                } catch (EOFException ex) {
                    // Done.
                    break;
                } catch (IOException | ClassNotFoundException ex) {
                    throw new RuntimeException(ex);
                }

                serverResponses.add(response);
            }
        }
    }
}

