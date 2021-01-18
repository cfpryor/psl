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

    protected float[] deltaModelGradient;
    protected float[] previousVariableValues;
    protected GroundAtom[] previousVariableAtoms;
    protected Map<Integer, Rule> deltaPages;

    public SGDOnlineTermStore(List<Rule> rules, AtomManager atomManager, SGDTermGenerator termGenerator) {
        super(rules, atomManager, termGenerator);
        deltaPages = new HashMap<Integer, Rule>();

        previousVariableAtoms = new GroundAtom[variableAtoms.length];
        System.arraycopy(variableAtoms, 0, previousVariableAtoms, 0, variableAtoms.length);

        previousVariableValues = new float[variableValues.length];
        System.arraycopy(variableValues, 0, previousVariableValues, 0, variableValues.length);

        deltaModelGradient = new float[variableValues.length * 2];
    }

    @Override
    protected StreamingIterator<SGDObjectiveTerm> getGroundingIterator() {
        return new SGDOnlineGroundingIterator(
                this, rules, atomManager, termGenerator,
                termCache, termPool, termBuffer, volatileBuffer, pageSize, numPages);
    }

    @Override
    protected StreamingIterator<SGDObjectiveTerm> getCacheIterator() {
        return new SGDStreamingCacheIterator(
                this, false, termCache, termPool,
                termBuffer, volatileBuffer, shufflePage, shuffleMap, randomizePageAccess, numPages);
    }

    @Override
    protected StreamingIterator<SGDObjectiveTerm> getNoWriteIterator() {
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

        int activePageIndex = 0;
        for (Integer i : rulePages) {
            activePageIndex = activeTermPages.indexOf(i);
            if (activePageIndex == -1) {
                activeTermPages.add(i);
                deltaPages.put(i, null);
                // This represents the number of active pages.
                numPages++;
            } else {
                log.warn("Page: {} already activated for rule: {}", i, rule.toString());
                log.warn("Active Term Pages: {}", activeTermPages);
            }
        }
        rules.add(rule);
        return rule;
    }

    @Override
    public Rule deactivateRule(Rule rule) {
        ArrayList<Integer> rulePages = pageMapping.get(rule);
        if (rulePages == null) {
            // No pages with rule.
            return null;
        }

        log.trace("Deactivating Pages: {}", rulePages);
        int activePageIndex = 0;
        for (Integer i : rulePages) {
            activePageIndex = activeTermPages.indexOf(i);
            if (activePageIndex != -1) {
                deltaPages.put(i, rule);
            } else {
                log.warn("Page: {} already deactivated for rule: {}", i, rule.toString());
                log.warn("Active Term Pages: {}", activeTermPages);
            }
        }
        return rule;
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

    public void clearDeltaPages(){
        // Deactivate the pages now.
        for (Integer i : deltaPages.keySet()){
            if (deltaPages.get(i) != null) {
                ((OnlineTermStore)this).deactivateRule(deltaPages.get(i));
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

    public void computeDeltaModelGradient(SGDObjectiveTerm term, boolean add){
        // Make sure there is room in delta model gradient
        if (variableValues.length > deltaModelGradient.length) {
            // Double the size of the array if realocation is required
            float[] tmpDeltaModelGradient = new float[deltaModelGradient.length * 2];
            System.arraycopy(deltaModelGradient, 0, tmpDeltaModelGradient, 0, deltaModelGradient.length);
            deltaModelGradient = tmpDeltaModelGradient;
        }

        term.deltaGradient(this, add);
    }

    public Map<Integer, Rule> getDeltaPages() {
        return  deltaPages;
    }

    public float getDeltaModelGradient(){
        float total = 0.0f;

        for (int i = 0; i < deltaModelGradient.length; i++) {
            total += (float) Math.pow(deltaModelGradient[i], 2.0f);
            deltaModelGradient[i] = 0.0f;
        }

        total = (float) Math.pow(total, 0.5f);
        return total;
    }

    public boolean deltaPagesEmpty() {
        return deltaPages.isEmpty();
    }

    public void updatePreviousVariables() {
        previousVariableAtoms = new GroundAtom[variableAtoms.length];
        System.arraycopy(variableAtoms, 0, previousVariableAtoms, 0, variableAtoms.length);

        previousVariableValues = new float[variableValues.length];
        System.arraycopy(variableValues, 0, previousVariableValues, 0, variableValues.length);
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
