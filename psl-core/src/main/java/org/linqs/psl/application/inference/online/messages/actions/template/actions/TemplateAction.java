package org.linqs.psl.application.inference.online.messages.actions.template.actions;

import org.linqs.psl.application.inference.online.messages.actions.OnlineAction;
import org.linqs.psl.model.atom.Atom;
import org.linqs.psl.model.predicate.ExternalFunctionalPredicate;
import org.linqs.psl.model.rule.Rule;

import java.util.HashSet;

public abstract class TemplateAction extends OnlineAction {
    protected Rule rule;

    public TemplateAction(Rule rule) {
        super();
        // Block attempt to serialize ExternalFunctionalPredicates.
        HashSet<Atom> atomSet = new HashSet<Atom>();
        rule.getRewritableGroundingFormula().getAtoms(atomSet);
        for (Atom atom: atomSet) {
            if (atom.getPredicate() instanceof ExternalFunctionalPredicate) {
                throw new UnsupportedOperationException(
                        String.format("ExternalFunctionalPredicates are not serializable. Caused by: %s, in rule: %s",
                                atom.getPredicate(), rule));
            }
        }
        this.rule = rule;
    }

    public Rule getRule() {
        return rule;
    }

    public Rule setRule(Rule rule) {
        this.rule = rule;
        return rule;
    }
}
