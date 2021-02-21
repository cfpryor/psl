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
import org.linqs.psl.reasoner.sgd.term.SGDOnlineTermStore;
import org.linqs.psl.reasoner.term.VariableTermStore;
import org.linqs.psl.reasoner.term.TermStore;
import org.linqs.psl.util.IteratorUtils;
import org.linqs.psl.util.MathUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;

/**
 * Uses an SGD optimization method to optimize its GroundRules.
 */
public class SGDReasoner extends Reasoner {
    private static final Logger log = LoggerFactory.getLogger(SGDReasoner.class);

    private int maxIterations;
    private int minIterations;

    private boolean watchMovement;
    private float movementThreshold;

    public SGDReasoner() {
        maxIterations = Options.SGD_MAX_ITER.getInt();
        minIterations = Options.SGD_MIN_ITER.getInt();

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
        double objective = 0.0;
        double initialObjective = 0.0;
        double oldObjective = Double.POSITIVE_INFINITY;
        double alphaMin = Double.POSITIVE_INFINITY;
        List<Float> betas = new ArrayList<Float>();
        Float[] betasSorted = null;
        float betaMedian = 0.0f;
        float betaMax = Float.NEGATIVE_INFINITY;
        float betaAvg = 0.0f;
        List<Float> ls = new ArrayList<Float>();
        Float[] lsSorted = null;
        float lMedian = 0.0f;
        float lMax = Float.NEGATIVE_INFINITY;
        float lAvg = 0.0f;
        float[] initialValues = null;
        float[] secondValues = null;
        float[] oldVariableValues1 = null;
        float[] oldVariableValues2 = null;
        float[] initialGradient = null;
        float[] secondGradient = null;
        float[] oldGradient1 = null;
        float[] oldGradient2 = null;

        long totalTime = 0;
        boolean converged = false;
        int iteration = 1;

        if (log.isTraceEnabled()) {
            // Computing the gradient for all the activated and deactivated pages
            if (termStore instanceof SGDOnlineTermStore && !((SGDOnlineTermStore) termStore).deltaPagesEmpty()) {
                for (SGDObjectiveTerm term : termStore) {
                    continue;
                }
                ((SGDOnlineTermStore) termStore).clearDeltaPages();
            }
        }

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
                    if (log.isTraceEnabled()) {
                        term.addGradient(oldGradient2, oldVariableValues2, termStore);
                    }
                }

                termCount++;
                movement += term.minimize(iteration, termStore);
            }

            termStore.iterationComplete();

            if (termCount != 0) {
                movement /= termCount;
            }

            converged = breakOptimization(iteration, objective, oldObjective, movement, termCount);

            if (log.isTraceEnabled()) {
                if (iteration == 1) {
                    // Initialize old variables values and oldGradients.
                    oldVariableValues2 = Arrays.copyOf(termStore.getVariableValues(), termStore.getVariableValues().length);
                    secondValues = Arrays.copyOf(oldVariableValues2, termStore.getVariableValues().length);
                    oldVariableValues1 = new float[termStore.getVariableValues().length];
                    oldGradient1 = new float[termStore.getVariableValues().length];
                    oldGradient2 = new float[termStore.getVariableValues().length];
                } else {
                    // Update Lipschitz constant estimators.
                    ls.add(MathUtils.pnorm(oldGradient2, 2));
                    lAvg += ls.get(ls.size() - 1);
                    lMax = Math.max(ls.get(ls.size() - 1), lMax);

                    if (iteration == 2) {
                        secondGradient = Arrays.copyOf(oldGradient2, termStore.getVariableValues().length);
                    } else {
                        // Update beta constant estimators.
                        float variableChange = MathUtils.pnorm(MathUtils.vectorDifference(oldVariableValues2, oldVariableValues1), 2);
                        if (variableChange != 0.0) {
                            betas.add(MathUtils.pnorm(MathUtils.vectorDifference(oldGradient2, oldGradient1), 2) / variableChange);
                            betaAvg += betas.get(betas.size() - 1);
                            betaMax = Math.max(betas.get(betas.size() - 1), betaMax);
                            alphaMin = Math.min(betas.get(betas.size() - 1), alphaMin);
                        }
                    }

                    // Update old variables values and oldGradients.
                    System.arraycopy(oldVariableValues2, 0, oldVariableValues1, 0, oldVariableValues2.length);
                    System.arraycopy(termStore.getVariableValues(), 0, oldVariableValues2, 0, oldVariableValues2.length);
                    System.arraycopy(oldGradient2, 0, oldGradient1, 0, oldGradient1.length);
                    Arrays.fill(oldGradient2, 0.0f);
                    oldObjective = objective;
                }
            } else {
                if (iteration == 1) {
                    // Initialize old variables values and oldGradients.
                    oldVariableValues2 = Arrays.copyOf(termStore.getVariableValues(), termStore.getVariableValues().length);
                } else {
                    // Update old variables values and oldGradients.
                    System.arraycopy(termStore.getVariableValues(), 0, oldVariableValues2, 0, oldVariableValues2.length);
                    oldObjective = objective;
                }
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

        if (log.isTraceEnabled()) {
            // Compute the initial gradient and objective.
            initialValues = new float[termStore.getVariableValues().length];
            initialGradient = new float[termStore.getVariableValues().length];
            for (int i = 0; i < termStore.getVariableAtoms().length; i++) {
                if (termStore.getVariableAtoms()[i] != null) {
                    initialValues[i] = termStore.getVariableAtoms()[i].getValue();
                }
            }

            for (SGDObjectiveTerm term : termStore) {
                term.addGradient(initialGradient, initialValues, termStore);
                initialObjective += term.evaluate(initialValues);
            }

            float variableChange = MathUtils.pnorm(MathUtils.vectorDifference(secondValues, initialValues), 2);
            if (variableChange != 0.0) {
                betas.add(0, MathUtils.pnorm(MathUtils.vectorDifference(secondGradient, initialGradient), 2) / variableChange);
                betaAvg += betas.get(0);
                betaMax = Math.max(betas.get(0), betaMax);
                alphaMin = Math.min(betas.get(0), alphaMin);
            }

            ls.add(0, MathUtils.pnorm(initialGradient, 2));
            lAvg += ls.get(0);
            lMax = Math.max(ls.get(0), lMax);
        }

        objective = computeObjective(termStore);

        if (log.isTraceEnabled()) {
            lAvg /= iteration;
            lsSorted = ls.toArray(new Float[0]);
            Arrays.sort(lsSorted);
            lMedian = lsSorted.length % 2 == 0 ?
                    (lsSorted[(int) (lsSorted.length / 2.0)] + lsSorted[(int) (lsSorted.length / 2 - 1)])
                    : lsSorted[(int) Math.floor(lsSorted.length / 2.0)];
            if (betas.size() != 0) {
                betaAvg /= (iteration - 1);
                betasSorted = betas.toArray(new Float[0]);
                Arrays.sort(betasSorted);
                betaMedian = betasSorted.length % 2 == 0 ?
                        (betasSorted[(int) (betasSorted.length / 2.0)] + betasSorted[(int) (betasSorted.length / 2.0 - 1)])
                        : betasSorted[(int) Math.floor(betasSorted.length / 2.0)];
            }
        }

        change = termStore.syncAtoms();

        log.info("Final Objective: {}, Final Normalized Objective: {}, Total Optimization Time: {}", objective, objective / termCount, totalTime);
        if (log.isTraceEnabled()) {
            log.trace("Initial Objective: {}", initialObjective);
            log.trace("Initial Normalized Objective: {}", initialObjective / termCount);
            log.trace("Minimum observed rate of change of gradients (Alpha min): {}", alphaMin);
            log.trace("Observed rates of change of gradients (Beta): {}", betas.toString());
            log.trace("Maximum observed rate of change of gradients (Beta max): {}", betaMax);
            log.trace("Maximum observed rate of change of gradients (Beta median): {}", betaMedian);
            log.trace("Average observed rate of change of gradients (Beta average): {}", betaAvg);
            log.trace("Observed rates of magnitudes of gradients (L): {}", ls.toString());
            log.trace("Maximum observed magnitude of gradients (L max): {}", lMax);
            log.trace("Maximum observed magnitude of gradients (L median): {}", lMedian);
            log.trace("Average observed magnitude of gradients (L average): {}", lAvg);
            log.trace("Initial observed magnitude of gradient (g_{x}): {}", MathUtils.pnorm(initialGradient, 2));
            log.trace("Final observed magnitude of gradient (g_{x^*}): {}", MathUtils.pnorm(oldGradient1, 2));
            log.trace("Movement of variables from initial state: {}", change);
        }
        log.debug("Optimized with {} variables and {} terms.", termStore.getNumVariables(), termCount);

        // Store atoms and values for the delta model.
        if (termStore instanceof SGDOnlineTermStore) {
            log.info("Delta model change in gradient: {}", ((SGDOnlineTermStore)termStore).getDeltaModelGradient());
            ((SGDOnlineTermStore)termStore).updatePreviousVariables();
        }

        return objective;
    }

    private boolean breakOptimization(int iteration, double objective, double oldObjective, float movement, long termCount) {
        // Always break when the allocated iterations is up.
        if (iteration > (int)(maxIterations * budget)) {
            return true;
        }

        // Never break when the allocated iterations less than minIterations.
        if (iteration < (int)(minIterations * budget)) {
            return false;
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
