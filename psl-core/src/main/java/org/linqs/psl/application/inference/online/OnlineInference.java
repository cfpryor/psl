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
import org.linqs.psl.application.inference.online.actions.*;
import org.linqs.psl.database.Database;
import org.linqs.psl.database.atom.OnlineAtomManager;
import org.linqs.psl.model.predicate.Predicate;
import org.linqs.psl.model.rule.GroundRule;
import org.linqs.psl.model.rule.Rule;
import org.linqs.psl.reasoner.sgd.term.SGDObjectiveTerm;
import org.linqs.psl.reasoner.sgd.term.SGDTermGenerator;
import org.linqs.psl.reasoner.term.OnlineTermStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Use streaming grounding and inference with an SGD reasoner.
 */
public abstract class OnlineInference extends InferenceApplication {
    private static final Logger log = LoggerFactory.getLogger(OnlineInference.class);

    public OnlineServer server;
    private boolean close;

    protected OnlineInference(List<Rule> rules, Database db) {
        super(rules, db);
        startServer();
        close = false;
    }

    protected OnlineInference(List<Rule> rules, Database db, boolean relaxHardConstraints) {
        super(rules, db, relaxHardConstraints);
        startServer();
        close = false;
    }

    /**
     * Get objects ready for inference.
     * This will call into the abstract method completeInitialize().
     */
    @Override
    protected void initialize() {
        log.debug("Creating persisted atom manager.");
        atomManager = createAtomManager(db);
        log.debug("Atom manager initialization complete.");

        initializeAtoms();

        reasoner = createReasoner();
        termStore = createTermStore();
        groundRuleStore = createGroundRuleStore();
        termGenerator = createTermGenerator();

        int atomCapacity = atomManager.getCachedRVACount() + atomManager.getCachedOBSCount();
        termStore.ensureVariableCapacity(atomCapacity);

        if (normalizeWeights) {
            normalizeWeights();
        }

        if (relaxHardConstraints) {
            relaxHardConstraints();
        }

        completeInitialize();
    }

    @Override
    protected abstract OnlineTermStore createTermStore();

    @Override
    protected OnlineAtomManager createAtomManager(Database db) {
        return new OnlineAtomManager(db);
    }

    private void startServer() {
        try {
            server = new OnlineServer<OnlineAction>();
            server.start();
        } catch (IOException e) {
            log.info("Failed to start server");
            close();
            System.exit(1);
        }
    }

    protected void executeAction(OnlineAction nextAction) throws IllegalArgumentException {
        // TODO: (Charles)
        //  switch or if else on classtypes
        switch (nextAction.getName()) {
            case "UpdateObservation":
                doUpdateObservation((UpdateObservation)nextAction);
                break;
            case "AddAtom":
                doAddAtom((AddAtom)nextAction);
                break;
            case "DeleteAtom":
                doDeleteAtom((DeleteAtom)nextAction);
                break;
            case "QueryAll":
                doQueryAll((QueryAll)nextAction);
                break;
            case "Close":
                doClose((Close)nextAction);
                break;
            default:
                throw new IllegalArgumentException("Action: " + nextAction.getName() + "Not Supported.");
        }
    }

    protected void doAddAtom(AddAtom nextAction) throws IllegalArgumentException {
        // Resolve Predicate
        Predicate registeredPredicate = Predicate.get(nextAction.getPredicateName());
        if (registeredPredicate == null) {
            throw new IllegalArgumentException("Predicate is not registered: " + nextAction.getPredicateName());
        }

        switch (nextAction.getPartitionName()) {
            case "READ":
                ((OnlineTermStore)termStore).addAtom(registeredPredicate, nextAction.getArguments(), nextAction.getValue(), true);
                break;
            case "WRITE":
                ((OnlineTermStore)termStore).addAtom(registeredPredicate, nextAction.getArguments(), nextAction.getValue(), false);
                break;
            default:
                throw new IllegalArgumentException("Add Atom Partition: " + nextAction.getPartitionName() + "Not Supported");

        }

        ArrayList<GroundRule> groundRules = ((OnlineAtomManager)atomManager).activateAtoms(rules, (OnlineTermStore) termStore);

        SGDTermGenerator termGenerator = new SGDTermGenerator();
        for (GroundRule groundRule : groundRules) {
            SGDObjectiveTerm newTerm = termGenerator.createTerm(groundRule, termStore);
            ((OnlineTermStore)termStore).addTerm(newTerm);
        }
    }

    protected void doDeleteAtom(DeleteAtom nextAction) throws IllegalArgumentException {
        // Resolve Predicate
        Predicate registeredPredicate = Predicate.get(nextAction.getPredicateName());
        if (registeredPredicate == null) {
            throw new IllegalArgumentException("Predicate is not registered: " + nextAction.getPredicateName());
        }

        ((OnlineTermStore)termStore).deleteAtom(registeredPredicate, nextAction.getArguments());
    }


    protected void doUpdateObservation(UpdateObservation nextAction) throws IllegalArgumentException {
        // Resolve Predicate
        Predicate registeredPredicate = Predicate.get(nextAction.getPredicateName());
        if (registeredPredicate == null) {
            throw new IllegalArgumentException("Predicate is not registered: " + nextAction.getPredicateName());
        }

        ((OnlineTermStore)termStore).updateAtom(registeredPredicate, nextAction.getArguments(), nextAction.getValue());
    }

    protected void doClose(Close nextAction) {
        close = true;
    }

    protected void doQueryAll(QueryAll nextAction) {
        reasoner.optimize(termStore);
    }

    /**
     * Minimize the total weighted incompatibility of the atoms according to the rules,
     * TODO: (Charles) By overriding internal inference rather than inference() we are not committing the random
     *  variable atom values to the data base after updates
     */
    @Override
    public void internalInference() {
        // Initial round of inference
        reasoner.optimize(termStore);

        OnlineAction nextAction;
        try {
            do {
                log.info("Waiting for next action from client");
                nextAction = (OnlineAction) server.dequeClientInput();
                log.info("Got next action from client. Executing: " + nextAction.getName());

                try {
                    executeAction(nextAction);
                    log.info("Executed Action: " + nextAction.getName());
                } catch (IllegalArgumentException | IllegalStateException e) {
                    log.info("Error thrown while executing action.");
                    log.info(e.getMessage());
                    log.info(e.toString());
                }
            } while (!close);
        } catch (InterruptedException e) {
            log.info("Internal Inference Interrupted");
            log.info(e.getMessage());
        } finally {
            server.closeServer();
        }

    }
}