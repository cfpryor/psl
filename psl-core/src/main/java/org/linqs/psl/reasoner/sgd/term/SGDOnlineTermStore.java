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
package org.linqs.psl.reasoner.sgd.term;

import org.linqs.psl.database.atom.AtomManager;
import org.linqs.psl.database.atom.OnlineAtomManager;
import org.linqs.psl.model.atom.GroundAtom;
import org.linqs.psl.model.atom.RandomVariableAtom;
import org.linqs.psl.model.predicate.StandardPredicate;
import org.linqs.psl.model.rule.Rule;
import org.linqs.psl.model.term.Constant;
import org.linqs.psl.reasoner.term.streaming.StreamingIterator;
import org.linqs.psl.reasoner.term.online.OnlineTermStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SGDOnlineTermStore extends OnlineTermStore<SGDObjectiveTerm> {
    private static final Logger log = LoggerFactory.getLogger(SGDOnlineTermStore.class);

    double[] deltaModelGradient;
    float[] previousVariableValues;
    GroundAtom[] previousVariableAtoms;
    // Pages of activated and deactivated term pages since last optimize call.
    private Map<Integer, Rule> deltaPages;
    // Pages of missing potentials from approximation.
    private ArrayList<Integer> approximationPages;

    public SGDOnlineTermStore(List<Rule> rules, AtomManager atomManager, SGDTermGenerator termGenerator) {
        super(rules, atomManager, termGenerator);
        deltaPages = new HashMap<Integer, Rule>();
        approximationPages = new ArrayList<Integer>();

        previousVariableAtoms = new GroundAtom[variableAtoms.length];
        System.arraycopy(variableAtoms, 0, previousVariableAtoms, 0, variableAtoms.length);

        previousVariableValues = new float[variableValues.length];
        System.arraycopy(variableValues, 0, previousVariableValues, 0, variableValues.length);

        deltaModelGradient = new double[variableValues.length * 2];
    }

    @Override
    public StreamingIterator<SGDObjectiveTerm> getGroundingIterator() {
        return new SGDOnlineGroundingIterator(
                this, rules, atomManager, termGenerator,
                termCache, termPool, termBuffer, volatileBuffer, pageSize, numPages);
    }

    @Override
    public StreamingIterator<SGDObjectiveTerm> getCacheIterator() {
        return new SGDStreamingCacheIterator(
                this, false, termCache, termPool,
                termBuffer, volatileBuffer, shufflePage, shuffleMap, randomizePageAccess, numPages);
    }

    @Override
    public StreamingIterator<SGDObjectiveTerm> getNoWriteIterator() {
        return new SGDStreamingCacheIterator(
                this, true, termCache, termPool,
                termBuffer, volatileBuffer, shufflePage, shuffleMap, randomizePageAccess, numPages);
    }

    @Override
    public Rule activateRule(Rule rule) {
        ArrayList<Integer> rulePages = pageMapping.get(rule);
        if (rulePages == null) {
            // No pages with rule.
            return null;
        }

        for (Integer pageIndex : rulePages) {
            if (!activatePage(pageIndex)) {
                log.warn("Page: {} already activated for rule: {}", pageIndex, rule.toString());
            }
        }

        rules.add(rule);
        return rule;
    }

    private Boolean activatePage(Integer pageIndex) {
        int activePageIndex = activeTermPages.indexOf(pageIndex);
        if (activePageIndex != -1) {
            return false;
        }

        activeTermPages.add(pageIndex);
        // This represents the number of active pages.
        numPages++;

        // If rule was deactivated and activated before an optimization then remove from delta pages.
        if (deltaPages.containsKey(pageIndex)) {
            deltaPages.remove(pageIndex);
        } else {
            deltaPages.put(pageIndex, null);
        }

        return true;
    }

    @Override
    public Rule deactivateRule(Rule rule) {
        ArrayList<Integer> rulePages = pageMapping.get(rule);
        if (rulePages == null) {
            // No pages with rule.
            return null;
        }

        log.trace("SGD ONLINE TERM STORE Deactivating Pages: {} from Active Term Pages: {}", rulePages, activeTermPages);
        for (Integer pageIndex : rulePages) {
            if (deactivatePage(pageIndex)) {
                // If rule was activated and deactivated before an optimization then remove from delta pages.
                if (deltaPages.containsKey(pageIndex)) {
                    deltaPages.remove(pageIndex);
                } else {
                    deltaPages.put(pageIndex, rule);
                }
            } else {
                log.warn("Page: {} already deactivated for rule: {}", pageIndex, rule.toString());
            }
        }
        return rule;
    }

    private Boolean deactivatePage(Integer pageIndex) {
        int activePageIndex = activeTermPages.indexOf(pageIndex);
        if (activePageIndex == -1) {
            return false;
        }

        activeTermPages.remove(pageIndex);
        numPages--;
        return true;
    }

    @Override
    public GroundAtom deleteAtom(StandardPredicate predicate, Constant[] arguments) {
        GroundAtom atom = ((OnlineAtomManager)atomManager).deleteAtom(predicate, arguments);

        if (atom == null) {
            // Atom never existed.
            return null;
        }

        int index = getVariableIndex(atom);
        if (index == -1) {
            // Atom never used in any terms.
            return atom;
        }

        variables.remove(atom);

        // For delta model logging.
        variableAtoms[index] = null;

        if (atom instanceof RandomVariableAtom) {
            numRandomVariableAtoms--;
        } else {
            numObservedAtoms--;
        }

        return atom;
    }

    public void clearDeltaPages() {
        // Deactivate the pages now.
        for (Integer i : deltaPages.keySet()){
            if (deltaPages.get(i) != null) {
                super.deactivateRule(deltaPages.get(i));
            }
        }
        deltaPages.clear();

        // Set variable values that were newly deleted to -1.
        for (int i = 0; i < variableAtoms.length; i++) {
            if (variableAtoms[i] == null) {
                variableValues[i] = -1;
            }
        }
    }

    public void computeDeltaModelGradient(SGDObjectiveTerm term, boolean add) {
        // Make sure there is room in delta model gradient
        if (variableValues.length > deltaModelGradient.length) {
            // Double the size of the array if realocation is required
            double[] tmpDeltaModelGradient = new double[deltaModelGradient.length * 2];
            System.arraycopy(deltaModelGradient, 0, tmpDeltaModelGradient, 0, deltaModelGradient.length);
            deltaModelGradient = tmpDeltaModelGradient;
        }

        term.deltaGradient(this, add);
    }

    public Map<Integer, Rule> getDeltaPages() {
        return  deltaPages;
    }

    public double getDeltaModelGradient(){
        double total = 0.0;

        for (int i = 0; i < deltaModelGradient.length; i++) {
            total += Math.pow(deltaModelGradient[i], 2.0);
            deltaModelGradient[i] = 0.0;
        }

        total = Math.pow(total, 0.5);
        return total;
    }

    public boolean deltaPagesEmpty() {
        return deltaPages.isEmpty();
    }

    public void activateApproximationPages() {
        for (Integer page_index : approximationPages) {
            activatePage(page_index);
        }
    }

    public void deactivateApproximationPages() {
        for (Integer page_index : approximationPages) {
            deactivatePage(page_index);
        }
    }

    public void addApproximationPages() {
        approximationPages.addAll(newTermPages);
    }

    public void updatePreviousVariables() {
        previousVariableAtoms = new GroundAtom[totalVariableCount];
        System.arraycopy(variableAtoms, 0, previousVariableAtoms, 0, totalVariableCount);

        previousVariableValues = new float[totalVariableCount];
        System.arraycopy(variableValues, 0, previousVariableValues, 0, totalVariableCount);
    }

    @Override
    public boolean rejectCacheTerm(SGDObjectiveTerm term) {
        boolean allObservedAtoms = true;

        for (int i=0; i < term.size(); i++) {
            if (variableAtoms[term.getVariableIndex(i)] == null) {
                // Calculate the delta model gradient for newly deleted atoms
                if (variableValues[term.getVariableIndex(i)] != -1) {
                    computeDeltaModelGradient(term, false);
                }
                return true;
            }

            // If a random variable atom is present in the term,
            // then the term contributes to optimization and should not be rejected.
            if (variableAtoms[term.getVariableIndex(i)] instanceof RandomVariableAtom) {
                allObservedAtoms = false;
            }
        }

        return allObservedAtoms;
    }
}
