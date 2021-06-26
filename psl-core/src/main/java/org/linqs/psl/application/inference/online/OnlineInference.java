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
import org.linqs.psl.application.inference.online.messages.actions.model.actions.AddAtom;
import org.linqs.psl.application.inference.online.messages.actions.model.actions.ObserveAtom;
import org.linqs.psl.application.inference.online.messages.actions.model.actions.DeleteAtom;
import org.linqs.psl.application.inference.online.messages.actions.controls.Stop;
import org.linqs.psl.application.inference.online.messages.actions.controls.Sync;
import org.linqs.psl.application.inference.online.messages.actions.model.actions.UpdateObservation;
import org.linqs.psl.application.inference.online.messages.actions.controls.QueryAtom;
import org.linqs.psl.application.inference.online.messages.actions.controls.WriteInferredPredicates;
import org.linqs.psl.application.inference.online.messages.actions.OnlineAction;
import org.linqs.psl.application.inference.online.messages.actions.template.actions.ActivateRule;
import org.linqs.psl.application.inference.online.messages.actions.template.actions.AddRule;
import org.linqs.psl.application.inference.online.messages.actions.template.actions.DeactivateRule;
import org.linqs.psl.application.inference.online.messages.actions.template.actions.DeleteRule;
import org.linqs.psl.application.inference.online.messages.responses.ActionStatus;
import org.linqs.psl.application.inference.online.messages.responses.QueryAtomResponse;
import org.linqs.psl.application.inference.online.messages.actions.OnlineActionException;
import org.linqs.psl.database.Database;
import org.linqs.psl.database.atom.PersistedAtomManager;
import org.linqs.psl.database.atom.OnlineAtomManager;
import org.linqs.psl.model.atom.GroundAtom;
import org.linqs.psl.model.atom.ObservedAtom;
import org.linqs.psl.model.atom.RandomVariableAtom;
import org.linqs.psl.model.rule.Rule;
import org.linqs.psl.reasoner.term.online.OnlineTermStore;
import org.linqs.psl.util.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public abstract class OnlineInference extends InferenceApplication {
    private static final Logger log = LoggerFactory.getLogger(OnlineInference.class);

    private OnlineServer server;

    private boolean modelUpdates;
    private boolean stopped;
    private double objective;
    private int variableChangeCount;
    private double variableChange;

    protected OnlineInference(List<Rule> rules, Database database) {
        super(rules, database);
    }

    protected OnlineInference(List<Rule> rules, Database database, boolean relaxHardConstraints) {
        super(rules, database, relaxHardConstraints);
    }

    @Override
    protected void initialize() {
        stopped = false;
        modelUpdates = true;
        objective = 0.0;
        variableChangeCount = 0;
        variableChange = 0.0;

        startServer();

        super.initialize();

        if (!(termStore instanceof OnlineTermStore)) {
            throw new RuntimeException("Online inference requires an OnlineTermStore. Found " + termStore.getClass() + ".");
        }
        termStore.ensureVariableCapacity(atomManager.getCachedRVACount() + atomManager.getCachedObsCount());
    }

    @Override
    protected PersistedAtomManager createAtomManager(Database database) {
        return new OnlineAtomManager(database, this.initialValue);
    }

    @Override
    public void close() {
        if (server != null) {
            server.close();
            server = null;
        }

        super.close();
    }

    private void startServer() {
        server = new OnlineServer(this.rules);
        server.start();
    }

    protected void executeAction(OnlineAction action) {
        String response = null;

        if (action.getClass() == ActivateRule.class) {
            response = doActivateRule((ActivateRule)action);
        } else if (action.getClass() == AddAtom.class) {
            response = doAddAtom((AddAtom)action);
        } else if (action.getClass() == AddRule.class) {
            response = doAddRule((AddRule)action);
        } else if (action.getClass() == DeactivateRule.class) {
            response = doDeactivateRule((DeactivateRule)action);
        } else if (action.getClass() == DeleteRule.class) {
            response = doDeleteRule((DeleteRule)action);
        } else if (action.getClass() == ObserveAtom.class) {
            response = doObserveAtom((ObserveAtom)action);
        } else if (action.getClass() == DeleteAtom.class) {
            response = doDeleteAtom((DeleteAtom)action);
        } else if (action.getClass() == Stop.class) {
            response = doStop((Stop)action);
        } else if (action.getClass() == Sync.class) {
            response = doSync((Sync)action);
        } else if (action.getClass() == UpdateObservation.class) {
            response = doUpdateObservation((UpdateObservation)action);
        } else if (action.getClass() == QueryAtom.class) {
            response = doQueryAtom((QueryAtom)action);
        } else if (action.getClass() == WriteInferredPredicates.class) {
            response = doWriteInferredPredicates((WriteInferredPredicates)action);
        } else {
            throw new OnlineActionException("Unknown action: " + action.getClass().getName() + ".");
        }

        server.onActionExecution(action, new ActionStatus(action, true, response));
    }

    protected String doAddAtom(AddAtom action) {
        boolean readPartition = (action.getPartitionName().equalsIgnoreCase("READ"));
        GroundAtom atom = null;

        if (atomManager.getDatabase().hasAtom(action.getPredicate(), action.getArguments())) {
            atom = ((OnlineAtomManager)atomManager).deleteAtom(action.getPredicate(), action.getArguments());
            atom = ((OnlineTermStore)termStore).deleteLocalVariable(atom);
        }

        if (readPartition) {
            atom = ((OnlineAtomManager)atomManager).addObservedAtom(action.getPredicate(), action.getValue(), action.getArguments());
        } else {
            atom = ((OnlineAtomManager)atomManager).addRandomVariableAtom(action.getPredicate(), action.getValue(), action.getArguments());
        }

        atom = ((OnlineTermStore)termStore).createLocalVariable(atom);

        modelUpdates = true;
        return String.format("Added atom: %s", atom.toStringWithValue());
    }

    protected String doActivateRule(ActivateRule action) {
        Rule rule = ((OnlineTermStore)termStore).activateRule(action.getRule());

        if (rule != null ) {
            modelUpdates = true;
            return String.format("Activated rule: %s", rule.toString());
        }

        return String.format("Rule: %s did not have any associated term pages.", action.getRule().toString());
    }

    protected String doAddRule(AddRule action) {
        Rule rule = ((OnlineTermStore)termStore).addRule(action.getRule());

        modelUpdates = true;
        return String.format("Added rule: %s", rule.toString());
    }

    protected String doDeactivateRule(DeactivateRule action) {
        Rule rule = ((OnlineTermStore)termStore).deactivateRule(action.getRule());

        if (rule != null ) {
            modelUpdates = true;
            return String.format("Deactivated rule: %s", rule.toString());
        }

        return String.format("Rule: %s did not have any associated term pages.", action.getRule().toString());
    }

    protected String doDeleteRule(DeleteRule action) {
        Rule rule = ((OnlineTermStore)termStore).deleteRule(action.getRule());

        if (rule != null ) {
            modelUpdates = true;
            return String.format("Deleted rule: %s", rule.toString());
        }

        return String.format("Rule: %s did not have any associated term pages.", action.getRule().toString());
    }

    protected String doObserveAtom(ObserveAtom action) {
        if (!atomManager.getDatabase().hasAtom(action.getPredicate(), action.getArguments())) {
            return String.format("Atom: %s(%s) did not exist in model.",
                    action.getPredicate(), StringUtils.join(", ", action.getArguments()));
        }

        GroundAtom atom = atomManager.getAtom(action.getPredicate(), action.getArguments());
        if (!(atom instanceof RandomVariableAtom)) {
            return String.format("Atom: %s(%s) already observed.",
                    action.getPredicate(), StringUtils.join(", ", action.getArguments()));
        }

        float oldAtomValue = atom.getValue();

        // Delete then create atom with same predicates and arguments as the random variable atom.
        ((OnlineAtomManager)atomManager).deleteAtom(action.getPredicate(), action.getArguments());
        ObservedAtom observedAtom = ((OnlineAtomManager)atomManager).addObservedAtom(action.getPredicate(), action.getValue(), action.getArguments());
        ObservedAtom newAtom = ((OnlineTermStore)termStore).updateLocalVariable(observedAtom, action.getValue());
        if (newAtom == null) {
            return String.format("Atom: %s(%s) did not exist in ground model.",
                    action.getPredicate(), StringUtils.join(", ", action.getArguments()));
        }

        modelUpdates = true;
        variableChangeCount++;
        variableChange += Math.pow(oldAtomValue - newAtom.getValue(), 2);

        return String.format("Observed atom: %s => %s", atom.toStringWithValue(), newAtom.toStringWithValue());
    }

    protected String doDeleteAtom(DeleteAtom action) {
        GroundAtom atom = ((OnlineAtomManager)atomManager).deleteAtom(action.getPredicate(), action.getArguments());

        if (atom == null) {
            return String.format("Atom: %s(%s) did not exist in atom manager.",
                    action.getPredicate(), StringUtils.join(", ", action.getArguments()));
        }

        atom = ((OnlineTermStore)termStore).deleteLocalVariable(atom);

        if (atom == null) {
            return String.format("Atom: %s(%s) did not exist in any terms of the model.",
                    action.getPredicate(), StringUtils.join(", ", action.getArguments()));
        }

        modelUpdates = true;
        return String.format("Deleted atom: %s", atom.toString());
    }

    protected String doStop(Stop action) {
        stopped = true;
        return "OnlinePSL inference stopped.";
    }

    protected String doSync(Sync action) {
        optimize();
        return "OnlinePSL inference synced.";
    }

    protected String doUpdateObservation(UpdateObservation action) {
        if (!atomManager.getDatabase().hasAtom(action.getPredicate(), action.getArguments())) {
            return String.format("Atom: %s(%s) did not exist in model.",
                    action.getPredicate(), StringUtils.join(", ", action.getArguments()));
        }

        GroundAtom atom = atomManager.getAtom(action.getPredicate(), action.getArguments());
        if (!(atom instanceof ObservedAtom)) {
            return String.format("Atom: %s(%s) is not an observation.",
                    action.getPredicate(), StringUtils.join(", ", action.getArguments()));
        }

        float oldAtomValue = atom.getValue();
        ObservedAtom updatedAtom = ((OnlineTermStore)termStore).updateLocalVariable((ObservedAtom)atom, action.getValue());
        if (updatedAtom == null) {
            return String.format("Atom: %s(%s) did not exist in ground model.",
                    action.getPredicate(), StringUtils.join(", ", action.getArguments()));
        }

        updatedAtom._assumeValue(action.getValue());
        modelUpdates = true;
        variableChangeCount ++;
        variableChange += Math.pow(oldAtomValue - updatedAtom.getValue(), 2);

        return String.format("Updated atom: %s: %f => %f", atom, oldAtomValue, updatedAtom.getValue());
    }

    protected String doWriteInferredPredicates(WriteInferredPredicates action) {
        String response = null;

        optimize();

        if (action.getOutputDirectoryPath() != null) {
            log.info("Writing inferred predicates to file: " + action.getOutputDirectoryPath());
            db.outputRandomVariableAtoms(action.getOutputDirectoryPath());
            response = "Wrote inferred predicates to file: " + action.getOutputDirectoryPath();
        } else {
            log.info("Writing inferred predicates to output stream.");
            db.outputRandomVariableAtoms();
            response = "Wrote inferred predicates to output stream.";
        }

        return response;
    }

    protected String doQueryAtom(QueryAtom action) {
        double atomValue = -1.0;

        optimize();

        if (((OnlineAtomManager)atomManager).hasAtom(action.getPredicate(), action.getArguments())) {
            atomValue = atomManager.getAtom(action.getPredicate(), action.getArguments()).getValue();
        }

        server.onActionExecution(action, new QueryAtomResponse(action, atomValue));

        if (atomValue == -1.0) {
            return String.format("Atom: %s(%s) not found.",
                    action.getPredicate(), StringUtils.join(", ", action.getArguments()));
        }

        return String.format("Atom: %s(%s) found. Returned to client.",
                action.getPredicate(), StringUtils.join(", ", action.getArguments()));
    }

    /**
     * Optimize if there were any new or deleted atoms since last optimization.
     */
    private void optimize() {
        if (!modelUpdates) {
            return;
        }

        log.debug("Model updates: (variable change count): {} unique variables", variableChangeCount);
        log.debug("Model updates: (variable delta): {}", Math.sqrt(variableChange));
        variableChangeCount = 0;
        variableChange = 0.0;

        log.trace("Optimization Start");
        objective = reasoner.optimize(termStore);
        log.trace("Optimization End");

        modelUpdates = false;
    }

    @Override
    public double internalInference() {
        // Initial round of inference.
        optimize();

        while (!stopped) {
            OnlineAction action = server.getAction();
            if (action == null) {
                continue;
            }

            try {
                executeAction(action);
            } catch (OnlineActionException ex) {
                server.onActionExecution(action, new ActionStatus(action, false, ex.getMessage()));
                log.warn("Exception when executing action: " + action, ex);
            } catch (RuntimeException ex) {
                server.onActionExecution(action, new ActionStatus(action, false, ex.getMessage()));
                throw new RuntimeException("Critically failed to run command. Last seen command: " + action, ex);
            }
        }

        return objective;
    }
}
