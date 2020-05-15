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
package org.linqs.psl.reasoner.term;

import org.linqs.psl.config.Options;
import org.linqs.psl.model.atom.GroundAtom;
import org.linqs.psl.model.rule.GroundRule;
import org.linqs.psl.util.RandUtils;

import java.util.ArrayList;
import java.util.Iterator;

public class MemoryTermStore<T extends ReasonerTerm> implements TermStore<T, GroundAtom> {
    private ArrayList<T> store;

    public MemoryTermStore() {
        this(Options.MEMORY_TS_INITIAL_SIZE.getInt());
    }

    public MemoryTermStore(int initialSize) {
        store = new ArrayList<T>(initialSize);
    }

    @Override
    public synchronized void add(GroundRule rule, T term) {
        store.add(term);
    }

    @Override
    public void clear() {
        if (store != null) {
            store.clear();
        }
    }

    @Override
    public void reset() {
        // Nothing is required for a MemoryTermStore to reset.
    }

    @Override
    public void close() {
        clear();

        store = null;
    }

    @Override
    public void initForOptimization() {
    }

    @Override
    public void iterationComplete() {
    }

    @Override
    public T get(int index) {
        return store.get(index);
    }

    @Override
    public int size() {
        return store.size();
    }

    @Override
    public void ensureTermCapacity(int capacity) {
        assert(capacity >= 0);

        if (capacity == 0) {
            return;
        }

        store.ensureCapacity(capacity);
    }

    @Override
    public void ensureAtomCapacity(int capacity) { }

    @Override
    public Iterator<T> iterator() {
        return store.iterator();
    }

    @Override
    public Iterator<T> noWriteIterator() {
        return iterator();
    }

    @Override
    public GroundAtom createLocalAtom(GroundAtom atom) {
        return atom;
    }

    public void shuffle() {
        RandUtils.shuffle(store);
    }
}
