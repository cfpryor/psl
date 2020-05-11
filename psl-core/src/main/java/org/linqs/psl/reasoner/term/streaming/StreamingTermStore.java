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
package org.linqs.psl.reasoner.term.streaming;

import org.linqs.psl.config.Options;
import org.linqs.psl.database.atom.AtomManager;
import org.linqs.psl.model.atom.ObservedAtom;
import org.linqs.psl.model.atom.RandomVariableAtom;
import org.linqs.psl.model.predicate.Predicate;
import org.linqs.psl.model.rule.GroundRule;
import org.linqs.psl.model.rule.Rule;
import org.linqs.psl.model.rule.WeightedRule;
import org.linqs.psl.model.term.Constant;
import org.linqs.psl.reasoner.InitialValue;
import org.linqs.psl.reasoner.term.HyperplaneTermGenerator;
import org.linqs.psl.reasoner.term.ReasonerTerm;
import org.linqs.psl.reasoner.term.VariableTermStore;
import org.linqs.psl.util.SystemUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * A term store that does not hold all the terms in memory, but instead keeps most terms on disk.
 * Variables are kept in memory, but terms are kept on disk.
 */
public abstract class StreamingTermStore<T extends ReasonerTerm> implements VariableTermStore<T, RandomVariableAtom> {
    private static final Logger log = LoggerFactory.getLogger(StreamingTermStore.class);

    public static final int INITIAL_PATH_CACHE_SIZE = 100;
    public static final int INITIAL_NEW_TERM_BUFFER_SIZE = 1000;

    protected List<WeightedRule> rules;
    protected AtomManager atomManager;

    // Keep track of variable and observation indexes.
    protected Map<RandomVariableAtom, Integer> variables;
    protected Map<ObservedAtom, Integer> observations;

    // Matching arrays for variables and observations values and atoms.
    private float[] variableValues;
    private RandomVariableAtom[] variableAtoms;
    private float[] observedValues;
    private ObservedAtom[] observedAtoms;

    // Buffer to hold new terms
    protected List<T> newTermBuffer;

    protected List<String> termPagePaths;
    protected List<String> volatilePagePaths;

    protected boolean initialRound;
    protected StreamingIterator<T> activeIterator;
    protected int seenTermCount;
    protected int numPages;

    protected HyperplaneTermGenerator<T, RandomVariableAtom> termGenerator;

    protected int pageSize;
    protected String pageDir;
    protected boolean shufflePage;
    protected boolean randomizePageAccess;

    protected boolean warnRules;

    protected Map<Integer, Float> atomsToUpdate;
    protected Map<Integer, Float> atomsUpdatingThisRound;

    /**
     * The IO buffer for terms.
     * This buffer is only written on the first iteration,
     * and contains only components of the terms that do not change.
     */
    protected ByteBuffer termBuffer;

    /**
     * The IO buffer for volatile values.
     * These values change every iteration, and need to be updated.
     */
    protected ByteBuffer volatileBuffer;

    /**
     * Terms in the current page.
     * On the initial round, this is filled from DB and flushed to disk.
     * On subsequent rounds, this is filled from disk.
     */
    protected List<T> termCache;

    /**
     * Terms that we will reuse when we start pulling from the cache.
     * This should be a fill page's worth.
     * After the initial round, terms will bounce between here and the term cache.
     */
    protected List<T> termPool;

    /**
     * When we shuffle pages, we need to know how they were shuffled so the volatile
     * cache can be writtten in the same order.
     * So we will shuffle this list of sequential ints in the same order as the page.
     */
    protected int[] shuffleMap;

    public StreamingTermStore(List<Rule> rules, AtomManager atomManager,
            HyperplaneTermGenerator<T, RandomVariableAtom> termGenerator) {
        pageSize = Options.STREAMING_TS_PAGE_SIZE.getInt();
        pageDir = Options.STREAMING_TS_PAGE_LOCATION.getString();
        shufflePage = Options.STREAMING_TS_SHUFFLE_PAGE.getBoolean();
        randomizePageAccess = Options.STREAMING_TS_RANDOMIZE_PAGE_ACCESS.getBoolean();
        warnRules = Options.STREAMING_TS_WARN_RULES.getBoolean();

        this.rules = new ArrayList<WeightedRule>();
        for (Rule rule : rules) {
            if (!rule.isWeighted()) {
                if (warnRules) {
                    log.warn("Streaming term stores do not support hard constraints: " + rule);
                }
                continue;
            }

            // HACK(eriq): This is not actually true,
            //  but I am putting it in place for efficiency reasons.
            if (((WeightedRule)rule).getWeight() < 0.0) {
                if (warnRules) {
                    log.warn("Streaming term stores do not support negative weights: " + rule);
                }
                continue;
            }

            if (!rule.supportsIndividualGrounding()) {
                if (warnRules) {
                    log.warn("Streaming term stores do not support rules that cannot individually ground (arithmetic rules with summations): " + rule);
                }
                continue;
            }

            if (!supportsRule(rule)) {
                if (warnRules) {
                    log.warn("Rule not supported: " + rule);
                }

                continue;
            }

            this.rules.add((WeightedRule)rule);
        }

        if (rules.size() == 0) {
            throw new IllegalArgumentException("Found no valid rules for a streaming term store.");
        }

        this.atomManager = atomManager;
        this.termGenerator = termGenerator;
        ensureVariableCapacity(atomManager.getCachedRVACount());
        ensureObservedCapacity(atomManager.getCachedOBSCount());

        termPagePaths = new ArrayList<String>(INITIAL_PATH_CACHE_SIZE);
        volatilePagePaths = new ArrayList<String>(INITIAL_PATH_CACHE_SIZE);
        newTermBuffer = new ArrayList<T>(INITIAL_NEW_TERM_BUFFER_SIZE);

        initialRound = true;
        activeIterator = null;
        numPages = 0;

        termBuffer = null;
        volatileBuffer = null;

        SystemUtils.recursiveDelete(pageDir);
        if (pageSize <= 1) {
            throw new IllegalArgumentException("Page size is too small.");
        }

        termCache = new ArrayList<T>(pageSize);
        termPool = new ArrayList<T>(pageSize);
        shuffleMap = new int[pageSize];

        atomsToUpdate = new HashMap<Integer, Float>();
        atomsUpdatingThisRound = new HashMap<Integer, Float>();

        (new File(pageDir)).mkdirs();
    }

    public boolean isLoaded() {
        return !initialRound;
    }

    public int getNumVariables() {
        return variables.size();
    }

    public Iterable<RandomVariableAtom> getVariables() {
        return variables.keySet();
    }

    public Iterable<ObservedAtom> getObservations() {
        return observations.keySet();
    }

    @Override
    public float[] getVariableValues() {
        return variableValues;
    }

    public float[] getObservedValues() {
        return observedValues;
    }

    @Override
    public float getVariableValue(int index) {
        return variableValues[index];
    }

    public float getObservedValue(int index) {
        return observedValues[index];
    }

    @Override
    public int getVariableIndex(RandomVariableAtom variable) {
        return variables.get(variable).intValue();
    }

    public int getObservedIndex(ObservedAtom observed) {
        return observations.get(observed).intValue();
    }

    @Override
    public void syncAtoms() {
        for (int i = 0; i < variables.size(); i++) {
            variableAtoms[i].setValue(variableValues[i]);
        }
    }

    @Override
    public synchronized RandomVariableAtom createLocalVariable(RandomVariableAtom atom) {
        if(variables.containsKey(atom)){
            return atom;
        }

        // Got a new variable.

        if (variables.size() >= variableAtoms.length) {
            ensureVariableCapacity(variables.size() * 2);
        }

        int index = variables.size();

        variables.put(atom, index);
        variableValues[index] = atom.getValue();
        variableAtoms[index] = atom;

        return atom;
    }

    public synchronized ObservedAtom createLocalObserved(ObservedAtom atom) {
        if (observations.containsKey(atom)) {
            return atom;
        }

        // Got a new variable.

        if (observations.size() >= observedAtoms.length) {
            ensureObservedCapacity(observations.size() * 2);
        }

        int index = observations.size();

        observations.put(atom, index);
        observedValues[index] = atom.getValue();
        observedAtoms[index] = atom;

        return atom;
    }

    public void ensureVariableCapacity(int capacity) {
        if (capacity < 0) {
            throw new IllegalArgumentException("Variable capacity must be non-negative. Got: " + capacity);
        }

        if (variables == null || variables.size() == 0) {
            // If there are no variables, then (re-)allocate the variable storage.
            // The default load factor for Java HashSets is 0.75.
            variables = new HashMap<RandomVariableAtom, Integer>((int)Math.ceil(capacity / 0.75));

            variableValues = new float[capacity];
            variableAtoms = new RandomVariableAtom[capacity];
        } else if (variables.size() < capacity) {
            // Don't bother with small reallocations, if we are reallocating make a lot of room.
            if (capacity < variables.size() * 2) {
                capacity = variables.size() * 2;
            }

            // Reallocate and copy over variables.
            Map<RandomVariableAtom, Integer> newVariables = new HashMap<RandomVariableAtom, Integer>((int)Math.ceil(capacity / 0.75));
            newVariables.putAll(variables);
            variables = newVariables;

            variableValues = Arrays.copyOf(variableValues, capacity);
            variableAtoms = Arrays.copyOf(variableAtoms, capacity);
        }
    }

    public HyperplaneTermGenerator<T, RandomVariableAtom> getTermGenerator(){
        return termGenerator;
    }

    public void ensureObservedCapacity(int capacity) {
        if (capacity < 0) {
            throw new IllegalArgumentException("Observed capacity must be non-negative. Got: " + capacity);
        }

        if (observations == null || observations.size() == 0) {
            // If there are no variables, then (re-)allocate the variable storage.
            // The default load factor for Java HashSets is 0.75.
            observations = new HashMap<ObservedAtom, Integer>((int)Math.ceil(capacity / 0.75));

            observedValues = new float[capacity];
            observedAtoms = new ObservedAtom[capacity];
        } else if (observations.size() < capacity) {
            // Don't bother with small reallocations, if we are reallocating make a lot of room.
            if (capacity < observations.size() * 2) {
                capacity = observations.size() * 2;
            }

            // Reallocate and copy over variables.
            Map<ObservedAtom, Integer> newObservations = new HashMap<>((int)Math.ceil(capacity / 0.75));
            newObservations.putAll(observations);
            observations = newObservations;

            observedValues = Arrays.copyOf(observedValues, capacity);
            observedAtoms = Arrays.copyOf(observedAtoms, capacity);
        }
    }

    public void rewrite(String termPagePath, List<T> newPageTerms) {
        // ToDo Implement rewriting of newTermPage
    }

    @Override
    public int size() {
        return seenTermCount;
    }

    @Override
    public void add(GroundRule rule, T term) {
        this.add(term);
    }

    public void add(T term) {
        //Currently a hack, newTermBuffer should be handled dynamically
        seenTermCount = seenTermCount + 1;
        newTermBuffer.add(term);
    }

    /**
     * Online Method
     * */
    public synchronized void updateObservationValue(ObservedAtom atom, float newValue){
        // add the atom and newValue to the updates map for cache iterator
        atomsToUpdate.put(observations.get(atom), newValue);
    }

    /**
     * Online Method
     * */
    public synchronized void updateObservationValue(Predicate predicate, Constant[] arguments, float newValue){
        // add the atom and newValue to the updates map for cache iterator
        ObservedAtom atom = (ObservedAtom)atomManager.getAtom(predicate, arguments);
        updateObservationValue(atom, newValue);
    }

    /**
     * Online Method
     * Cache iterator calls this method to determine if the term needs updating
     * */
    public boolean updateTerm(T term){
        return false;
    }

    @Override
    public T get(int index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void ensureCapacity(int capacity) {
        throw new UnsupportedOperationException();
    }

    public String getTermPagePath(int index) {
        // Make sure the path is built.
        for (int i = termPagePaths.size(); i <= index; i++) {
            termPagePaths.add(Paths.get(pageDir, String.format("%08d_term.page", i)).toString());
        }

        return termPagePaths.get(index);
    }

    public String getVolatilePagePath(int index) {
        // Make sure the path is built.
        for (int i = volatilePagePaths.size(); i <= index; i++) {
            volatilePagePaths.add(Paths.get(pageDir, String.format("%08d_volatile.page", i)).toString());
        }

        return volatilePagePaths.get(index);
    }

    /**
     * A callback for the initial round iterator.
     * The ByterBuffers are here because of possible reallocation.
     */
    public void initialIterationComplete(int termCount, int numPages, ByteBuffer termBuffer, ByteBuffer volatileBuffer) {
        seenTermCount = termCount;
        this.numPages = numPages;
        this.termBuffer = termBuffer;
        this.volatileBuffer = volatileBuffer;

        initialRound = false;
        activeIterator = null;
    }

    /**
     * A callback for the non-initial round iterator.
     */
    public synchronized void cacheIterationComplete() {
        activeIterator = null;

        // clear the updates for this round
        atomsUpdatingThisRound.clear();
    }

    /**
     * Get an iterator that goes over all the terms for only reading.
     * Before this method can be called, a full iteration must have already been done.
     * (The cache will need to have been built.)
     */
    public Iterator<T> noWriteIterator() {
        if (activeIterator != null) {
            throw new IllegalStateException("Iterator already exists for this StreamingTermStore. Exhaust the iterator first.");
        }

        if (initialRound) {
            throw new IllegalStateException("A full iteration must have already been completed before asking for a read-only iterator.");
        }

        activeIterator = getNoWriteIterator();

        return activeIterator;
    }

    @Override
    public synchronized Iterator<T> iterator() {
        if (activeIterator != null) {
            throw new IllegalStateException("Iterator already exists for this StreamingTermStore. Exhaust the iterator first.");
        }

        if (initialRound) {
            activeIterator = getInitialRoundIterator();
        } else {
            // update atomsUpdatingThisRound for the iterator and clear the atomsToUpdate
            for (Map.Entry<Integer, Float>entry : atomsToUpdate.entrySet()) {
                atomsUpdatingThisRound.put(entry.getKey(), entry.getValue());
            }
            atomsToUpdate.clear();

            activeIterator = getCacheIterator();
        }

        return activeIterator;
    }

    @Override
    public void clear() {
        initialRound = true;
        numPages = 0;

        if (activeIterator != null) {
            activeIterator.close();
            activeIterator = null;
        }

        if (variables != null) {
            variables.clear();
        }

        if (termCache != null) {
            termCache.clear();
        }

        if (termPool != null) {
            termPool.clear();
        }

        SystemUtils.recursiveDelete(pageDir);
    }

    @Override
    public void reset() {
        for (int i = 0; i < variables.size(); i++) {
            variableValues[i] = variableAtoms[i].getValue();
        }
    }

    @Override
    public void close() {
        clear();

        if (variables != null) {
            variables = null;
        }

        if (termBuffer != null) {
            termBuffer.clear();
            termBuffer = null;
        }

        if (volatileBuffer != null) {
            volatileBuffer.clear();
            volatileBuffer = null;
        }

        if (termCache != null) {
            termCache = null;
        }

        if (termPool != null) {
            termPool = null;
        }
    }

    @Override
    public void initForOptimization() {
    }

    @Override
    public void iterationComplete() {
    }

    /**
     * Check if this term store supports this rule.
     * @return true if the rule is supported.
     */
    protected abstract boolean supportsRule(Rule rule);

    /**
     * Get an iterator that will perform grounding queries and write the initial pages to disk.
     */
    protected abstract StreamingIterator<T> getInitialRoundIterator();

    /**
     * Get an iterator that will read and write from disk.
     */
    protected abstract StreamingIterator<T> getCacheIterator();

    /**
     * Get an iterator that will not write to disk.
     */
    protected abstract StreamingIterator<T> getNoWriteIterator();
}
