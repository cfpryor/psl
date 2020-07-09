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
import org.linqs.psl.grounding.PartialGrounding;
import org.linqs.psl.model.atom.GroundAtom;
import org.linqs.psl.model.predicate.Predicate;
import org.linqs.psl.model.rule.GroundRule;
import org.linqs.psl.model.rule.Rule;
import org.linqs.psl.reasoner.term.streaming.StreamingIterator;
import org.linqs.psl.reasoner.term.streaming.StreamingTermStore;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * A term store that iterates over ground queries directly (obviating the GroundRuleStore).
 * Note that the iterators given by this class are meant to be exhausted (at least the first time).
 * Remember that this class will internally iterate over an unknown number of groundings.
 * So interrupting the iteration can cause the term count to be incorrect.
 */
public class SGDStreamingTermStore extends StreamingTermStore<SGDObjectiveTerm> {
    public SGDStreamingTermStore(List<Rule> rules, AtomManager atomManager, SGDTermGenerator sgdTermGenerator) {
        super(rules, atomManager, sgdTermGenerator);
    }

    @Override
    protected boolean supportsRule(Rule rule) {
        // No special requirements for rules.
        return true;
    }

    @Override
    protected StreamingIterator<SGDObjectiveTerm> getGroundingIterator() {
        if (initialRound) {
            return new SGDStreamingGroundingIterator(
                    this, this.rules, atomManager, termGenerator,
                    termCache, termPool, termBuffer, volatileBuffer, pageSize, numPages, true);
        } else {
            Set<GroundAtom> newAtoms = ((OnlineAtomManager)atomManager).flushNewAtoms();
            ArrayList<? extends Rule> rules = new ArrayList(PartialGrounding.getLazyRules(this.rules, PartialGrounding.getOnlinePredicates(newAtoms)));

            return new SGDStreamingGroundingIterator(
                    this, rules, atomManager, termGenerator,
                    termCache, termPool, termBuffer, volatileBuffer, pageSize, numPages, false);
        }
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
    public boolean deletedTerm(SGDObjectiveTerm term) {
        int[] indices = term.getIndices();
        boolean[] deletedAtoms = getDeletedAtoms();

        for (int index: indices) {
            if(deletedAtoms[index]) {
                return true;
            }
        }

        return false;
    }
}
