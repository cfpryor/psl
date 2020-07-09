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

import org.linqs.psl.application.inference.InferenceApplication;
import org.linqs.psl.application.inference.online.actions.AddAtom;
import org.linqs.psl.application.inference.online.actions.Close;
import org.linqs.psl.application.inference.online.actions.DeleteAtom;
import org.linqs.psl.application.inference.online.actions.UpdateObservation;
import org.linqs.psl.application.inference.online.actions.WriteInferredPredicates;
import org.linqs.psl.application.inference.online.actions.OnlineAction;
import org.linqs.psl.config.Options;
import org.linqs.psl.database.Database;
import org.linqs.psl.database.atom.OnlineAtomManager;
import org.linqs.psl.model.predicate.Predicate;
import org.linqs.psl.model.rule.GroundRule;
import org.linqs.psl.model.rule.Rule;
import org.linqs.psl.reasoner.term.OnlineTermStore;
import org.linqs.psl.reasoner.term.ReasonerTerm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;


/**
 * Use streaming grounding and inference with an SGD reasoner.
 */
public abstract class OnlineInference extends InferenceApplication {
    private static final Logger log = LoggerFactory.getLogger(OnlineInference.class);

    public OnlineServer server;
    private boolean close;
    private double objective;

    protected OnlineInference(List<Rule> rules, Database db) {
        super(rules, db);
    }

    protected OnlineInference(List<Rule> rules, Database db, boolean relaxHardConstraints) {
        super(rules, db, relaxHardConstraints);
    }

    /**
     * Get objects ready for inference.
     * This will call into the abstract method completeInitialize().
     */
    @Override
    protected void initialize() {
        startServer();

        log.debug("Creating persisted atom manager.");
        atomManager = createAtomManager(db);
        log.debug("Atom manager initialization complete.");

        initializeAtoms();

        reasoner = createReasoner();
        termGenerator = createTermGenerator();
        termStore = createTermStore();
        groundRuleStore = createGroundRuleStore();

        int atomCapacity = atomManager.getCachedRVACount() + atomManager.getCachedOBSCount();
        termStore.ensureVariableCapacity(atomCapacity);

        if (normalizeWeights) {
            normalizeWeights();
        }

        if (relaxHardConstraints) {
            relaxHardConstraints();
        }

        close = false;
        objective = 0;

        completeInitialize();
    }

    @Override
    protected OnlineTermStore createTermStore() {
        return (OnlineTermStore)Options.INFERENCE_TS.getNewObject();
    }

    @Override
    protected OnlineAtomManager createAtomManager(Database db) {
        return new OnlineAtomManager(db);
    }

    private void startServer() {
        try {
            server = new OnlineServer<OnlineAction>();
            server.start();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> server.closeServer()));
        } catch (IOException e) {
            log.info("Failed to start server");
            System.exit(1);
        }
    }

    protected void executeAction(OnlineAction action) throws OnlineException {
        if (action.getClass() == UpdateObservation.class) {
            doUpdateObservation((UpdateObservation)action);
        } else if (action.getClass() == AddAtom.class) {
            doAddAtom((AddAtom)action);
        } else if (action.getClass() == DeleteAtom.class) {
            doDeleteAtom((DeleteAtom)action);
        } else if (action.getClass() == WriteInferredPredicates.class) {
            doWriteInferredPredicates((WriteInferredPredicates)action);
        } else if (action.getClass() == Close.class) {
            doClose((Close)action);
        } else {
            throw new OnlineException("Action: " + action.getClass().getName() + " not Supported.");
        }
    }

    protected void doAddAtom(AddAtom action) throws OnlineException {
        // Resolve Predicate
        Predicate registeredPredicate = Predicate.get(action.getPredicateName());
        if (registeredPredicate == null) {
            throw new OnlineException("Predicate is not registered: " + action.getPredicateName());
        }

        switch (action.getPartitionName()) {
            case "READ":
                ((OnlineTermStore)termStore).addAtom(registeredPredicate, action.getArguments(), action.getValue(), true);
                break;
            case "WRITE":
                ((OnlineTermStore)termStore).addAtom(registeredPredicate, action.getArguments(), action.getValue(), false);
                break;
            default:
                throw new OnlineException("Add Atom Partition: " + action.getPartitionName() + " not Supported");
        }
    }

    protected void doDeleteAtom(DeleteAtom action) throws OnlineException {
        // Resolve Predicate
        Predicate registeredPredicate = Predicate.get(action.getPredicateName());
        if (registeredPredicate == null) {
            throw new OnlineException("Predicate is not registered: " + action.getPredicateName());
        }

        ((OnlineTermStore)termStore).deleteAtom(registeredPredicate, action.getArguments());
    }

    protected void doUpdateObservation(UpdateObservation action) throws OnlineException {
        // Resolve Predicate
        Predicate registeredPredicate = Predicate.get(action.getPredicateName());
        if (registeredPredicate == null) {
            throw new OnlineException("Predicate is not registered: " + action.getPredicateName());
        }

        ((OnlineTermStore)termStore).updateAtom(registeredPredicate, action.getArguments(), action.getValue());
    }

    protected void doClose(Close action) {
        close = true;
    }

    protected void doWriteInferredPredicates(WriteInferredPredicates action) {
        log.trace("Optimization Start");
        objective = reasoner.optimize(termStore);
        log.trace("Optimization Start");

        if (action.getOutputDirectoryPath() != null) {
            log.info("Writing inferred predicates to file: " + action.getOutputDirectoryPath());
            db.outputRandomVariableAtoms(action.getOutputDirectoryPath());
        } else {
            log.info("Writing inferred predicates to output stream.");
            db.outputRandomVariableAtoms(System.out);
        }
    }

    /**
     * Minimize the total weighted incompatibility of the atoms according to the rules,
     * TODO: (Charles) By overriding internal inference rather than inference() we are not committing the random
     *  variable atom values to the database after updates. Perhaps periodically update the database or add it
     *  as a part of action execution.
     */
    @Override
    public double internalInference() {
        // Initial round of inference
        objective = reasoner.optimize(termStore);

        OnlineAction action = null;
        try {
            do {
                action = (OnlineAction) server.dequeClientInput();
                try {
                    executeAction(action);
                } catch (OnlineException ex) {
                    log.warn(String.format("Exception when executing action: %s", action), ex);
                } catch (RuntimeException ex) {
                    throw new RuntimeException("Critically failed to run command. Last seen command: " + action, ex);
                }
            } while (!close);
        } catch (InterruptedException ex) {
            log.warn("Internal Inference Interrupted.", ex);
        } finally {
            server.closeServer();
        }

        return objective;
    }
}

class OnlineException extends Exception {
    public OnlineException(String s) {
        super(s);
    }

    public OnlineException(String s, Exception ex) {
        super(s, ex);
    }
}