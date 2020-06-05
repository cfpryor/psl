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
package org.linqs.psl.reasoner.sgd;

import org.linqs.psl.config.Options;
import org.linqs.psl.model.atom.GroundAtom;
import org.linqs.psl.reasoner.Reasoner;
import org.linqs.psl.reasoner.sgd.term.SGDObjectiveTerm;
import org.linqs.psl.reasoner.term.VariableTermStore;
import org.linqs.psl.reasoner.term.TermStore;
import org.linqs.psl.util.IteratorUtils;
import org.linqs.psl.util.MathUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

/**
 * Uses an SGD optimization method to optimize its GroundRules.
 */
public class SGDReasoner extends Reasoner {
    private static final Logger log = LoggerFactory.getLogger(SGDReasoner.class);

    private int maxIterations;

    private boolean watchMovement;
    private float movementThreshold;

    public SGDReasoner() {
        maxIterations = Options.SGD_MAX_ITER.getInt();

        watchMovement = Options.SGD_MOVEMENT.getBoolean();
        movementThreshold = Options.SGD_MOVEMENT_THRESHOLD.getFloat();
    }

    @Override
    public void optimize(TermStore baseTermStore) {
        if (!(baseTermStore instanceof VariableTermStore)) {
            throw new IllegalArgumentException("SGDReasoner requires a AtomTermStore (found " + baseTermStore.getClass().getName() + ").");
        }

        @SuppressWarnings("unchecked")
        VariableTermStore<SGDObjectiveTerm, GroundAtom> termStore = (VariableTermStore<SGDObjectiveTerm, GroundAtom>)baseTermStore;

        termStore.initForOptimization();

        float objective = -1.0f;
        float oldObjective = Float.POSITIVE_INFINITY;

        if (printInitialObj && log.isTraceEnabled()) {
            objective = computeObjective(termStore);
            log.trace("Iteration {} -- Objective: {}, Mean Movement: {}, Iteration Time: {}, Total Optimization Time: {}", 0, objective, 0.0f, 0, 0);
        }

        int iteration = 1;
        long totalTime = 0;
        while (true) {
            long start = System.currentTimeMillis();

            // Keep track of the mean movement of the random variables.
            float movement = 0.0f;

            float[] variableValues = termStore.getVariableValues();
            GroundAtom[] variableAtoms = termStore.getVariableAtoms();
            for (SGDObjectiveTerm term : termStore) {
                movement += term.minimize(iteration, variableValues, variableAtoms);
            }

            // TODO(Charles): If in online mode then "variable values" contains the observed atoms as well.
            //    This means that this movement calculation differs depending on the termstore.
            //    This shouldn't be the case.
            if (variableValues.length != 0) {
                movement /= termStore.getNumVariables();
            }

            long end = System.currentTimeMillis();

            oldObjective = objective;
            objective = computeObjective(termStore);
            totalTime += end - start;

            if (log.isTraceEnabled()) {
                log.trace("Iteration {} -- Objective: {}, Mean Movement: {}, Iteration Time: {}, Total Optimization Time: {}",
                        iteration, objective, movement, (end - start), totalTime);
            }

            iteration++;
            termStore.iterationComplete();

            if (breakOptimization(iteration, objective, oldObjective, movement)) {
                break;
            }
        }

        termStore.syncAtoms();

        log.info("Optimization completed in {} iterations. Objective: {}, Total Optimization Time: {}",
                iteration - 1, objective, totalTime);
        log.debug("Optimized with {} variables and {} terms.", termStore.getNumVariables(), termStore.size());
    }

    private boolean breakOptimization(int iteration, float objective, float oldObjective, float movement) {
        // Always break when the allocated iterations is up.
        if (iteration > (int)(maxIterations * budget)) {
            return true;
        }

        // Do not break if there is too much movement.
        if (watchMovement && movement > movementThreshold) {
            return false;
        }

        // Break if the objective has not changed.
        if (objectiveBreak && MathUtils.equals(objective, oldObjective, tolerance)) {
            return true;
        }

        return false;
    }

    public float computeObjective(VariableTermStore<SGDObjectiveTerm, GroundAtom> termStore) {
        float objective = 0.0f;

        // If possible, use a readonly iterator.
        Iterator<SGDObjectiveTerm> termIterator = null;
        if (termStore.isLoaded()) {
            termIterator = termStore.noWriteIterator();
        } else {
            termIterator = termStore.iterator();
        }

        float[] variableValues = termStore.getVariableValues();
        int term_count = 0;
        for (SGDObjectiveTerm term : IteratorUtils.newIterable(termIterator)) {
            objective += term.evaluate(variableValues);
            term_count += 1;
        }

        return objective / term_count;
    }

    @Override
    public void close() {
    }
}
