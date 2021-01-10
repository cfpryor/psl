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

import java.util.Arrays;
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
    public double optimize(TermStore baseTermStore) {
        if (!(baseTermStore instanceof VariableTermStore)) {
            throw new IllegalArgumentException("SGDReasoner requires a VariableTermStore (found " + baseTermStore.getClass().getName() + ").");
        }

        @SuppressWarnings("unchecked")
        VariableTermStore<SGDObjectiveTerm, GroundAtom> termStore = (VariableTermStore<SGDObjectiveTerm, GroundAtom>)baseTermStore;

        termStore.initForOptimization();

        long termCount = 0;
        float movement = 0.0f;
        double change = 0.0;
        double objective = Double.POSITIVE_INFINITY;
        double oldObjective = Double.POSITIVE_INFINITY;
        double alphaMin = Double.NEGATIVE_INFINITY;
        double beta = 0.0;
        double betaMax = Double.NEGATIVE_INFINITY;
        double betaAvg = 0.0;
        double l = 0.0;
        double lMax = Double.NEGATIVE_INFINITY;
        double lAvg = 0.0;
        float[] initialValues = null;
        float[] oldVariableValues1 = null;
        float[] oldVariableValues2 = null;
        float[] initialGradient = null;
        float[] oldGradient1 = null;
        float[] oldGradient2 = null;

        long totalTime = 0;
        boolean converged = false;
        int iteration = 1;
        for (; iteration < (maxIterations * budget) && !converged; iteration++) {
            long start = System.currentTimeMillis();

            termCount = 0;
            movement = 0.0f;
            objective = 0.0;

            for (SGDObjectiveTerm term : termStore) {
                // Starting the second round of iteration, keep track of the old objective.
                // Note that the number of variables may change in the first iteration.
                if (iteration > 1) {
                    objective += term.evaluate(oldVariableValues2);
                    term.addGradient(oldGradient2, oldVariableValues2, termStore);
                }

                termCount++;
                movement += term.minimize(iteration, termStore);
            }

            termStore.iterationComplete();

            if (termCount != 0) {
                movement /= termCount;
            }

            converged = breakOptimization(iteration, objective, oldObjective, movement, termCount);

            if (iteration == 1) {
                // Initialize old variables values and oldGradients.
                oldVariableValues2 = Arrays.copyOf(termStore.getVariableValues(), termStore.getVariableValues().length);
                oldVariableValues1 = new float[termStore.getVariableValues().length];
                oldGradient1 = new float[termStore.getVariableValues().length];
                oldGradient2 = new float[termStore.getVariableValues().length];
            } else {
                // Update Lipschitz constant estimators.
                l = MathUtils.pnorm(oldGradient2, 2);
                lAvg += l;
                lMax = Math.max(l, lMax);

                if (iteration > 2) {
                    // Update beta constant estimators.
                    double variableChange = MathUtils.pnorm(MathUtils.vectorDifference(oldVariableValues2, oldVariableValues1), 2);
                    if (variableChange != 0.0) {
                        beta = MathUtils.pnorm(MathUtils.vectorDifference(oldGradient2, oldGradient1), 2) / variableChange;
                        betaAvg += beta;
                        betaMax = Math.max(beta, betaMax);
                        alphaMin = Math.min(beta, alphaMin);
                    }
                }

                // Update old variables values and oldGradients.
                System.arraycopy(oldVariableValues2, 0, oldVariableValues1, 0, oldVariableValues1.length);
                System.arraycopy(termStore.getVariableValues(), 0, oldVariableValues2, 0, oldVariableValues2.length);
                System.arraycopy(oldGradient2, 0, oldGradient1, 0, oldGradient1.length);
                Arrays.fill(oldGradient2, 0.0f);
                oldObjective = objective;
            }

            long end = System.currentTimeMillis();
            totalTime += System.currentTimeMillis() - start;

            if (iteration > 1) {
                if (log.isTraceEnabled()) {
                    log.trace("Iteration {} -- Objective: {}, Normalized Objective: {}, Iteration Time: {}, Total Optimization Time: {}",
                            iteration - 1, objective, objective / termCount, (end - start), totalTime);
                }
            }
        }

        // Compute the initial gradient.
        initialValues = new float[termStore.getVariableValues().length];
        initialGradient = new float[termStore.getVariableValues().length];
        for (int i = 0; i < termStore.getVariableAtoms().length; i ++) {
            if (termStore.getVariableAtoms()[i] != null) {
                initialValues[i] = termStore.getVariableAtoms()[i].getValue();
            }
        }

        for (SGDObjectiveTerm term : termStore) {
            term.addGradient(initialGradient, initialValues, termStore);
        }

        l = MathUtils.pnorm(initialGradient, 2);
        lAvg += l;
        lMax = Math.max(l, lMax);

        objective = computeObjective(termStore);
        lAvg /= iteration;
        betaAvg /= (iteration - 2);
        change = termStore.syncAtoms();

        log.info("Final Objective: {}, Final Normalized Objective: {}, Total Optimization Time: {}", objective, objective / termCount, totalTime);
        log.info("Minimum observed rate of change of gradients (Alpha min): {}", alphaMin);
        log.info("Maximum observed rate of change of gradients (Beta max): {}", betaMax);
        log.info("Average observed rate of change of gradients (Beta average): {}", betaAvg);
        log.info("Maximum observed magnitude of gradients (L max): {}", lMax);
        log.info("Average observed magnitude of gradients (L average): {}", lAvg);
        log.info("Initial observed magnitude of gradient (g_{x^*}): {}", MathUtils.pnorm(initialGradient, 2));
        log.info("Final observed magnitude of gradient (g_{x^*}): {}", MathUtils.pnorm(oldGradient2, 2));
        log.info("Movement of variables from initial state: {}", change);
        log.debug("Optimized with {} variables and {} terms.", termStore.getNumVariables(), termCount);

        return objective;
    }

    private boolean breakOptimization(int iteration, double objective, double oldObjective, float movement, long termCount) {
        // Always break when the allocated iterations is up.
        if (iteration > (int)(maxIterations * budget)) {
            return true;
        }

        // Run through the maximum number of iterations.
        if (runFullIterations) {
            return false;
        }

        // Do not break if there is too much movement.
        if (watchMovement && movement > movementThreshold) {
            return false;
        }

        // Break if the objective has not changed.
        if (oldObjective != Double.POSITIVE_INFINITY && objectiveBreak && MathUtils.equals(objective / termCount, oldObjective / termCount, tolerance)) {
            return true;
        }

        return false;
    }

    private double computeObjective(VariableTermStore<SGDObjectiveTerm, GroundAtom> termStore) {
        double objective = 0.0;

        // If possible, use a readonly iterator.
        Iterator<SGDObjectiveTerm> termIterator = null;
        if (termStore.isLoaded()) {
            termIterator = termStore.noWriteIterator();
        } else {
            termIterator = termStore.iterator();
        }

        float[] variableValues = termStore.getVariableValues();
        for (SGDObjectiveTerm term : IteratorUtils.newIterable(termIterator)) {
            objective += term.evaluate(variableValues);
        }

        return objective;
    }

    @Override
    public void close() {
    }
}
