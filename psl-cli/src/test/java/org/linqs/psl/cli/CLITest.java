/*
 * This file is part of the PSL software.
 * Copyright 2011-2015 University of Maryland
 * Copyright 2013-2021 The Regents of the University of California
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

import org.linqs.psl.model.predicate.Predicate;
import org.linqs.psl.parser.CommandLineLoader;

import org.junit.After;
import org.junit.Before;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * The base class for tests that run a PSL model via the CLI.
 */
public abstract class CLITest {
    public static final String PREFIX = "cli_test";
    public static final String RESOURCES_BASE_FILE = ".resources";
    public static final String BASE_DATA_DIR_NAME = "data";
    public static final String BASE_MODELS_DIR_NAME = "models";
    public static final String BASE_ONLINE_ACTIONS_DIR_NAME = "onlineActions";
    public static final String SERVER_TEMP_FILE_NAME_PREFIX = "OnlinePSLServer";
    public static final String SERVER_TEMP_FILE_NAME_SUFFIX = ".tmp";

    protected final String outDir;
    protected final String resourceDir;
    protected final String baseDataDir;
    protected final String baseModelsDir;
    protected final String baseOnlineActionsDir;
    protected final String serverTempFileDir;

    public CLITest() {
        outDir = Paths.get(System.getProperty("java.io.tmpdir"), PREFIX + "_" + this.getClass().getName()).toString();

        resourceDir = (new File(this.getClass().getClassLoader().getResource(RESOURCES_BASE_FILE).getFile())).getParentFile().getAbsolutePath();
        baseDataDir = Paths.get(resourceDir, BASE_DATA_DIR_NAME).toString();
        baseModelsDir = Paths.get(resourceDir, BASE_MODELS_DIR_NAME).toString();
        baseOnlineActionsDir = Paths.get(resourceDir, BASE_ONLINE_ACTIONS_DIR_NAME).toString();
        serverTempFileDir= Paths.get(System.getProperty("java.io.tmpdir")).toString();
    }

    @Before
    public void setUp() {
        (new File(outDir)).mkdirs();
    }

    @After
    public void tearDown() {
        (new File(outDir)).delete();
        Predicate.clearForTesting();
    }

    public void run(String modelPath, String dataPath) {
        run(modelPath, dataPath, null);
    }

    public void run(String modelPath, String dataPath, List<String> additionalArgs) {
        run(modelPath, dataPath, "OFF", additionalArgs);
    }

    public void run(String modelPath, String dataPath, String loggingLevel, List<String> additionalArgs) {
        List<String> args = new ArrayList<String>();
        args.add("--" + CommandLineLoader.OPERATION_INFER_LONG);

        args.add("--" + CommandLineLoader.OPTION_MODEL_LONG);
        args.add(modelPath);

        args.add("--" + CommandLineLoader.OPTION_DATA_LONG);
        args.add(dataPath);

        args.add("--" + CommandLineLoader.OPTION_OUTPUT_DIR_LONG);
        args.add(outDir);

        // Set the logging level.
        args.add("-" + CommandLineLoader.OPTION_PROPERTIES);
        args.add("log4j.threshold=" + loggingLevel);

        if (additionalArgs != null) {
            args.addAll(additionalArgs);
        }

        Launcher.main(args.toArray(new String[0]), true);

        validate();
    }

    public void runOnline(String modelPath, String dataPath, String actionPath) {
        runOnline(modelPath, dataPath, actionPath, null);
    }

    public void runOnline(String modelPath, String dataPath, String actionPath, List<String> additionalArgs) {
        runOnline(modelPath, dataPath, actionPath, "OFF", additionalArgs);
    }

    public void runOnline(String modelPath, String dataPath, String actionPath, String loggingLevel, List<String> additionalArgs) {
        Thread serverThread = null;
        Thread clientThread = null;
        List<String> serverArgs = new ArrayList<String>();
        List<String> clientArgs = new ArrayList<String>();
        File actionFile = new File(actionPath);
        File tempDir = null;
        File[] tempDirFiles = null;
        String serverTempFileName = null;

        // Set the inference application.
        serverArgs.add("--" + CommandLineLoader.OPERATION_INFER_LONG);
        serverArgs.add("SGDOnlineInference");

        // Set the model, data, and output directory for server.
        serverArgs.add("--" + CommandLineLoader.OPTION_MODEL_LONG);
        serverArgs.add(modelPath);

        serverArgs.add("--" + CommandLineLoader.OPTION_DATA_LONG);
        serverArgs.add(dataPath);

        serverArgs.add("--" + CommandLineLoader.OPTION_OUTPUT_DIR_LONG);
        serverArgs.add(outDir);

        // Set the logging level for server.
        serverArgs.add("-" + CommandLineLoader.OPTION_PROPERTIES);
        serverArgs.add("log4j.threshold=" + loggingLevel);

        // Start server.
        Runnable serverTask = () -> {
            Launcher.main(serverArgs.toArray(new String[0]), true);
        };

        serverThread = new Thread(serverTask);
        serverThread.start();

        // Wait for .tmp file to drop.
        boolean exists = false;
        do {
            tempDir = new File(serverTempFileDir);
            tempDirFiles = tempDir.listFiles();

            for (int i = 0; i < tempDirFiles.length; i++) {
                if (tempDirFiles[i].isFile()) {
                    serverTempFileName = tempDirFiles[i].getName();
                    if(serverTempFileName.startsWith(SERVER_TEMP_FILE_NAME_PREFIX)
                            && serverTempFileName.endsWith(SERVER_TEMP_FILE_NAME_SUFFIX)) {
                        exists = true;
                    }
                }
            }

            try {
                Thread.sleep(1000);
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            }
        } while (!exists);

        // Set the client option.
        clientArgs.add("--" + CommandLineLoader.OPERATION_ONLINE_CLIENT_LONG);

        // Set the client output directory.
        clientArgs.add("--" + CommandLineLoader.OPTION_OUTPUT_DIR_LONG);
        clientArgs.add(outDir);

        // Set the logging level for client.
        clientArgs.add("-" + CommandLineLoader.OPTION_PROPERTIES);
        clientArgs.add("log4j.threshold=" + loggingLevel);

        if (additionalArgs != null) {
            clientArgs.addAll(additionalArgs);
        }

        Runnable clientTask = () -> {
            try (
                    InputStream clientInputStream = new FileInputStream(actionFile)) {
                InputStream stdin = System.in;
                System.setIn(clientInputStream);
                Launcher.main(clientArgs.toArray(new String[0]), true);
                System.setIn(stdin);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        };

        clientThread = new Thread(clientTask);
        clientThread.start();

        try {
            clientThread.join();
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }

        try {
            serverThread.join();
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }

        validate();
    }

    /**
     * Children should override this if they want to do specific validations.
     */
    public void validate() {
        // The base implementation does nothing.
    }
}
