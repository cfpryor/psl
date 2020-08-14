package org.linqs.psl.application.inference.online.messages.responses;

import org.linqs.psl.application.inference.online.messages.actions.QueryAtom;
import org.linqs.psl.model.predicate.StandardPredicate;
import org.linqs.psl.model.term.Constant;
import org.linqs.psl.util.StringUtils;

import java.util.UUID;

public class QueryAtomResponse extends OnlineResponse {
    private double atomValue;
    private StandardPredicate predicate;
    private Constant[] arguments;

    public QueryAtomResponse(QueryAtom onlineAction, double atomValue) {
        super(UUID.randomUUID(), String.format(
                "Query\t%s\t%s\t%s\t%f",
                onlineAction.getIdentifier(),
                onlineAction.getPredicate().getName(),
                StringUtils.join("\t", onlineAction.getArguments()).replace("'", ""),
                atomValue));
    }

    public QueryAtomResponse(UUID identifier, String serverResponse) {
        super(identifier, serverResponse);
    }

    public double getAtomValue() {
        return atomValue;
    }

    public StandardPredicate getPredicate() {
        return predicate;
    }

    public Constant[] getArguments() {
        return arguments;
    }

    @Override
    public void setMessage(String newMessage) {
        parse(newMessage.split("\t"));

        message = String.format(
                "Query\t%s\t%s\t%s\t%f",
                onlineActionID,
                predicate.getName(),
                StringUtils.join("\t", arguments).replace("'", ""),
                atomValue);
    }

    private void parse(String[] parts) {
        assert(parts[0].equalsIgnoreCase("query"));

        onlineActionID = UUID.fromString(parts[1].trim());

        AtomInfo atomInfo = parseAtom(parts, 2);
        predicate = atomInfo.predicate;
        arguments = atomInfo.arguments;
        atomValue = Double.parseDouble(parts[parts.length - 1].trim());
    }
}
