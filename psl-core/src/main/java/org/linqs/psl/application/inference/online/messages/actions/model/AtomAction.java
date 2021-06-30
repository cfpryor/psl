package org.linqs.psl.application.inference.online.messages.actions.model;

import org.linqs.psl.application.inference.online.messages.OnlineMessage;
import org.linqs.psl.model.predicate.StandardPredicate;
import org.linqs.psl.model.term.Constant;

public class AtomAction extends OnlineMessage {
    protected StandardPredicate predicate;
    protected Constant[] arguments;

    public AtomAction(StandardPredicate predicate, Constant[] arguments) {
        this.predicate = predicate;
        this.arguments = arguments;
    }

    public StandardPredicate getPredicate() {
        return predicate;
    }

    public Constant[] getArguments() {
        return arguments;
    }
}
