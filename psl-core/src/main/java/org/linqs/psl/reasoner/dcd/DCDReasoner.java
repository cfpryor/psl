/*
 * This file is part of the PSL software.
 * Copyright 2011-2015 University of Maryland
 * Copyright 2013-2019 The Regents of the University of California
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
package org.linqs.psl.reasoner.dcd;

import org.linqs.psl.config.Config;
import org.linqs.psl.model.atom.RandomVariableAtom;
import org.linqs.psl.reasoner.Reasoner;
import org.linqs.psl.reasoner.dcd.term.DCDObjectiveTerm;
import org.linqs.psl.reasoner.dcd.term.DCDTermStore;
import org.linqs.psl.reasoner.term.TermStore;
import org.linqs.psl.util.MathUtils;
import org.linqs.psl.util.RandUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Uses an ADMM optimization method to optimize its GroundRules.
 */
public class DCDReasoner implements Reasoner {
    private static final Logger log = LoggerFactory.getLogger(DCDReasoner.class);

    /**
     * Prefix of property keys used by this class.
     */
    public static final String CONFIG_PREFIX = "dcd";

    /**
     * The maximum number of iterations of ADMM to perform in a round of inference.
     */
    public static final String MAX_ITER_KEY = CONFIG_PREFIX + ".maxiterations";
    public static final int MAX_ITER_DEFAULT = 200;

    /**
     * Stop if the objective has not changed since the last logging period (see LOG_PERIOD).
     */
    public static final String OBJECTIVE_BREAK_KEY = CONFIG_PREFIX + ".objectivebreak";
    public static final boolean OBJECTIVE_BREAK_DEFAULT = true;

    /**
     * The maximum number of iterations of ADMM to perform in a round of inference.
     */
    public static final String OBJ_TOL = CONFIG_PREFIX + ".tol";
    public static final float OBJ_TOL_DEFAULT = 0.000001f;

    public static final String C = CONFIG_PREFIX + ".C";
    public static final float C_DEFAULT = 10.0f;

    public static final String TRUNCATE_EVERY_STEP = CONFIG_PREFIX + ".truncateeverystep";
    public static final boolean TRUNCATE_EVERY_STEP_DEFAULT = false;

    public static final String PRINT_OBJECTIVE = CONFIG_PREFIX + ".printobj";
    public static final boolean PRINT_OBJECTIVE_DEFAULT = true;

    private int maxIter;

    private float tol;
    private boolean printObj;
    private boolean objectiveBreak;
    private float c;

    public DCDReasoner() {
        maxIter = Config.getInt(MAX_ITER_KEY, MAX_ITER_DEFAULT);
        objectiveBreak = Config.getBoolean(OBJECTIVE_BREAK_KEY, OBJECTIVE_BREAK_DEFAULT);
        printObj = Config.getBoolean(PRINT_OBJECTIVE, PRINT_OBJECTIVE_DEFAULT);
        tol = Config.getFloat(OBJ_TOL, OBJ_TOL_DEFAULT);
        c = Config.getFloat(C, C_DEFAULT);
    }

    public int getMaxIter() {
        return maxIter;
    }

    public void setMaxIter(int maxIter) {
        this.maxIter = maxIter;
    }

    @Override
    public void optimize(TermStore baseTermStore) {
        if (!(baseTermStore instanceof DCDTermStore)) {
            throw new IllegalArgumentException("DCDReasoner requires an DCDTermStore (found " + baseTermStore.getClass().getName() + ").");
        }
        DCDTermStore termStore = (DCDTermStore)baseTermStore;

        int numTerms = termStore.size();
        int numVariables = termStore.getNumVariables();

        log.debug("Performing optimization with {} variables and {} terms.", numVariables, numTerms);

        if (numTerms == 0) {
            log.warn("No terms found. DCD is existing early.");
            return;
        }

        // Initialize all variables to a random state.
        for (RandomVariableAtom variable : termStore.getVariables()) {
            variable.setValue(RandUtils.nextFloat());
        }

        float objective = computeObjective(termStore);
        float oldObjective = Float.POSITIVE_INFINITY;

        int iteration = 1;
        if (printObj) {
            log.trace("gretThis:Iterations,Time(ms),Objective");
            log.trace("grepThis:{},{},{}", iteration - 1, 0, objective);
        }

        float time = 0.0f;
        while (iteration <= maxIter
                && (!objectiveBreak || (iteration == 1 || !MathUtils.equals(objective, oldObjective, tol)))) {
            long start = System.currentTimeMillis();

            for (DCDObjectiveTerm term : termStore){
                term.minimize();
            }

            for (RandomVariableAtom variable : termStore.getVariables()) {
                variable.setValue(Math.max(Math.min(variable.getValue(), 1.0f), 0.0f));
            }

            long end = System.currentTimeMillis();
            oldObjective = objective;
            objective = computeObjective(termStore);
            time += end - start;

            if (printObj) {
                log.trace("grepThis:{},{},{}", iteration, time, objective);
            }

            iteration++;
        }

        log.info("Optimization completed in {} iterations. Objective.: {}", iteration - 1, objective);
    }

    public float computeObjective(DCDTermStore termStore) {
        float objective = 0.0f;
        int termCount = 0;

        for (DCDObjectiveTerm term : termStore) {
            objective += term.evaluate() / c;
            termCount++;
        }

        return objective / termCount;
    }

    @Override
    public void close() {
    }
}