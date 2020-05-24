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
package org.linqs.psl.application.inference.mpe;

import org.linqs.psl.database.Database;
import org.linqs.psl.grounding.GroundRuleStore;
import org.linqs.psl.model.atom.GroundAtom;
import org.linqs.psl.model.rule.Rule;
import org.linqs.psl.reasoner.Reasoner;
import org.linqs.psl.reasoner.sgd.SGDReasoner;
import org.linqs.psl.reasoner.sgd.term.SGDObjectiveTerm;
import org.linqs.psl.reasoner.sgd.term.SGDStreamingTermStore;
import org.linqs.psl.reasoner.term.OnlineTermStore;
import org.linqs.psl.reasoner.term.TermGenerator;
import org.linqs.psl.reasoner.term.TermStore;

import java.util.List;

/**
 * Use streaming grounding and inference with an SGD reasoner.
 */
public class SGDOnlineInference extends MPEOnlineInference {
    public SGDOnlineInference(List<Rule> rules, Database db) {
        super(rules, db);
    }

    @Override
    protected Reasoner createReasoner() {
        return new SGDReasoner();
    }

    @Override
    protected OnlineTermStore<SGDObjectiveTerm, GroundAtom> createTermStore() {
        return new SGDStreamingTermStore(rules, atomManager);
    }

    @Override
    protected GroundRuleStore createGroundRuleStore() {
        return null;
    }

    // Note that the SGDStreamingTermStore class has a class TermGenerator
    @Override
    protected TermGenerator createTermGenerator() {
        return null;
    }

    @Override
    public void close() {
        termStore.close();
        reasoner.close();

        termStore = null;
        reasoner = null;

        rules = null;
        db = null;
    }

    @Override
    protected void completeInitialize() {
        // Do nothing else. Specifically, do not ground.
    }
}
