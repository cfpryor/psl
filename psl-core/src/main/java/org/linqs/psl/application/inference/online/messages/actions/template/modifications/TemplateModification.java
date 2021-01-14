package org.linqs.psl.application.inference.online.messages.actions.template.modifications;

import org.linqs.psl.application.inference.online.messages.actions.OnlineAction;
import org.linqs.psl.model.rule.Rule;

public abstract class TemplateModification extends OnlineAction {
    protected Rule rule;

    public TemplateModification(Rule rule) {
        super();
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
