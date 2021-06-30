package org.linqs.psl.parser;

import org.linqs.psl.application.inference.online.messages.actions.OnlineAction;
import org.linqs.psl.application.inference.online.messages.actions.controls.Exit;
import org.linqs.psl.application.inference.online.messages.actions.controls.QueryAtom;
import org.linqs.psl.application.inference.online.messages.actions.controls.Stop;
import org.linqs.psl.application.inference.online.messages.actions.controls.Sync;
import org.linqs.psl.application.inference.online.messages.actions.controls.WeightLearn;
import org.linqs.psl.application.inference.online.messages.actions.controls.WriteInferredPredicates;
import org.linqs.psl.application.inference.online.messages.actions.model.actions.AddAtom;
import org.linqs.psl.application.inference.online.messages.actions.model.actions.DeleteAtom;
import org.linqs.psl.application.inference.online.messages.actions.model.actions.ObserveAtom;
import org.linqs.psl.application.inference.online.messages.actions.model.actions.UpdateObservation;
import org.linqs.psl.application.inference.online.messages.actions.template.actions.ActivateRule;
import org.linqs.psl.application.inference.online.messages.actions.template.actions.AddRule;
import org.linqs.psl.application.inference.online.messages.actions.template.actions.DeactivateRule;
import org.linqs.psl.application.inference.online.messages.actions.template.actions.DeleteRule;
import org.linqs.psl.model.atom.Atom;
import org.linqs.psl.model.predicate.StandardPredicate;
import org.linqs.psl.model.rule.Rule;
import org.linqs.psl.model.term.Constant;
import org.linqs.psl.parser.antlr.OnlinePSLBaseVisitor;
import org.linqs.psl.parser.antlr.OnlinePSLLexer;
import org.linqs.psl.parser.antlr.OnlinePSLParser;
import org.linqs.psl.parser.antlr.OnlinePSLParser.ActionContext;
import org.linqs.psl.parser.antlr.OnlinePSLParser.AddAtomContext;
import org.linqs.psl.parser.antlr.OnlinePSLParser.AddRuleContext;
import org.linqs.psl.parser.antlr.OnlinePSLParser.DeleteRuleContext;
import org.linqs.psl.parser.antlr.OnlinePSLParser.ActivateRuleContext;
import org.linqs.psl.parser.antlr.OnlinePSLParser.DeactivateRuleContext;
import org.linqs.psl.parser.antlr.OnlinePSLParser.DeleteAtomContext;
import org.linqs.psl.parser.antlr.OnlinePSLParser.ExitContext;
import org.linqs.psl.parser.antlr.OnlinePSLParser.NumberContext;
import org.linqs.psl.parser.antlr.OnlinePSLParser.ObserveAtomContext;
import org.linqs.psl.parser.antlr.OnlinePSLParser.OnlineProgramContext;
import org.linqs.psl.parser.antlr.OnlinePSLParser.UpdateObservationContext;
import org.linqs.psl.parser.antlr.OnlinePSLParser.QueryAtomContext;
import org.linqs.psl.parser.antlr.OnlinePSLParser.StopContext;
import org.linqs.psl.parser.antlr.OnlinePSLParser.SyncContext;
import org.linqs.psl.parser.antlr.OnlinePSLParser.WeightLearnContext;
import org.linqs.psl.parser.antlr.OnlinePSLParser.WriteInferredPredicatesContext;

import org.antlr.v4.runtime.BailErrorStrategy;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.misc.ParseCancellationException;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.LinkedList;
import java.util.List;

public class OnlineActionLoader extends OnlinePSLBaseVisitor<Object> {

    /**
     * Parse and return a single OnlineAction.
     * If exactly one action is not specified, an exception is thrown.
     */
    public static OnlineAction loadAction(String input) {
        List<OnlineAction> actions = load(new StringReader(input));

        if (actions.size() != 1) {
            throw new IllegalArgumentException(String.format("Expected 1 action, found %d.", actions.size()));
        }

        return actions.get(0);
    }

    /**
     * Convenience interface to load().
     */
    public static List<OnlineAction> load(String input) {
        return load(new StringReader(input));
    }

    /**
     * Parse and return a list of onlineActions.
     * If exactly one rule is not specified, an exception is thrown.
     */
    public static List<OnlineAction> load(Reader input) {
        OnlinePSLParser parser = null;
        try {
            parser = getParser(input);
        } catch (IOException ex) {
            // Cancel the lex and rethrow.
            throw new RuntimeException("Failed to lex action.", ex);
        }

        OnlineProgramContext onlineProgram = null;
        try {
            onlineProgram = parser.onlineProgram();
        } catch (ParseCancellationException ex) {
            // Cancel the parse and rethrow the cause.
            throw (RuntimeException)ex.getCause();
        }

        OnlineActionLoader visitor = new OnlineActionLoader();
        return visitor.visitOnlineProgram(onlineProgram, parser);
    }

    /**
     * Get a parser over the given input.
     */
    private static OnlinePSLParser getParser(Reader input) throws IOException {
        OnlinePSLLexer lexer = new OnlinePSLLexer(CharStreams.fromReader(input));

        // We need to add a error listener to the lexer so we halt on lex errors.
        lexer.addErrorListener(new BaseErrorListener() {
            @Override
            public void syntaxError(
                    Recognizer<?, ?> recognizer,
                    Object offendingSymbol,
                    int line,
                    int charPositionInLine,
                    String msg,
                    RecognitionException ex) throws ParseCancellationException {
                throw new ParseCancellationException("line " + line + ":" + charPositionInLine + " " + msg, ex);
            }
        });
        CommonTokenStream tokens = new CommonTokenStream(lexer);

        OnlinePSLParser parser = new OnlinePSLParser(tokens);
        parser.setErrorHandler(new BailErrorStrategy());

        return parser;
    }

    OnlineActionLoader() {}

    public List<OnlineAction> visitOnlineProgram(OnlinePSLParser.OnlineProgramContext ctx, OnlinePSLParser parser) {
        LinkedList<OnlineAction> actions = new LinkedList<OnlineAction>();
        for (ActionContext actionCtx : ctx.action()) {
            try {
                actions.add((OnlineAction)visit(actionCtx));
            } catch (RuntimeException ex) {
                throw new RuntimeException("Failed to compile online action: [" + parser.getTokenStream().getText(actionCtx) + "]", ex);
            }
        }
        return actions;
    }

    @Override
    public OnlineAction visitAction(ActionContext ctx) {
        if (ctx.addAtom() != null) {
            return visitAddAtom(ctx.addAtom());
        } else if (ctx.addRule() != null) {
            return visitAddRule(ctx.addRule());
        } else if (ctx.deleteRule() != null) {
            return visitDeleteRule(ctx.deleteRule());
        } else if (ctx.activateRule() != null) {
            return visitActivateRule(ctx.activateRule());
        } else if (ctx.deactivateRule() != null) {
            return visitDeactivateRule(ctx.deactivateRule());
        } else if (ctx.deleteAtom() != null) {
            return visitDeleteAtom(ctx.deleteAtom());
        } else if (ctx.exit() != null) {
            return visitExit(ctx.exit());
        } else if (ctx.observeAtom() != null) {
            return visitObserveAtom(ctx.observeAtom());
        } else if (ctx.queryAtom() != null) {
            return visitQueryAtom(ctx.queryAtom());
        } else if (ctx.stop() != null) {
            return visitStop(ctx.stop());
        } else if (ctx.sync() != null) {
            return visitSync(ctx.sync());
        } else if (ctx.updateObservation() != null) {
            return visitUpdateObservation(ctx.updateObservation());
        } else if (ctx.weightLearn() != null) {
            return visitWeightLearn(ctx.weightLearn());
        } else if (ctx.writeInferredPredicates() != null) {
            return visitWriteInferredPredicates(ctx.writeInferredPredicates());
        } else {
            throw new IllegalStateException();
        }
    }

    @Override
    public AddAtom visitAddAtom(AddAtomContext ctx) {
        Atom atom = ModelLoader.loadAtom(ctx.atom().getText());
        Constant[] constants = new Constant[atom.getArguments().length];
        for (int i = 0; i < constants.length; i++) {
            constants[i] = (Constant)atom.getArguments()[i];
        }
        String partition = ctx.PARTITION().getText();
        float value = -1.0f;
        if (ctx.number() != null) {
            value = visitNumber(ctx.number());
        }

        return new AddAtom(partition, (StandardPredicate)atom.getPredicate(), constants, value);
    }

    @Override
    public AddRule visitAddRule(AddRuleContext ctx) {
        Rule rule = ModelLoader.loadRule(ctx.pslRule().getText());

        return new AddRule(rule);
    }

    @Override
    public DeleteRule visitDeleteRule(DeleteRuleContext ctx) {
        Rule rule = ModelLoader.loadRule(ctx.pslRule().getText());

        return new DeleteRule(rule);
    }

    @Override
    public ActivateRule visitActivateRule(ActivateRuleContext ctx) {
        Rule rule = ModelLoader.loadRule(ctx.pslRule().getText());

        return new ActivateRule(rule);
    }

    @Override
    public DeactivateRule visitDeactivateRule(DeactivateRuleContext ctx) {
        Rule rule = ModelLoader.loadRule(ctx.pslRule().getText());

        return new DeactivateRule(rule);
    }

    @Override
    public Exit visitExit(ExitContext ctx) {
        return new Exit();
    }

    @Override
    public DeleteAtom visitDeleteAtom(DeleteAtomContext ctx) {
        Atom atom = ModelLoader.loadAtom(ctx.atom().getText());
        Constant[] constants = new Constant[atom.getArguments().length];
        for (int i = 0; i < constants.length; i++) {
            constants[i] = (Constant)atom.getArguments()[i];
        }
        String partition = ctx.PARTITION().getText();

        return new DeleteAtom(partition, (StandardPredicate)atom.getPredicate(), constants);
    }

    @Override
    public ObserveAtom visitObserveAtom(ObserveAtomContext ctx) {
        Atom atom = ModelLoader.loadAtom(ctx.atom().getText());
        Constant[] constants = new Constant[atom.getArguments().length];
        for (int i = 0; i < constants.length; i++) {
            constants[i] = (Constant)atom.getArguments()[i];
        }
        float value = visitNumber(ctx.number());

        return new ObserveAtom((StandardPredicate)atom.getPredicate(), constants, value);
    }

    @Override
    public UpdateObservation visitUpdateObservation(UpdateObservationContext ctx) {
        Atom atom = ModelLoader.loadAtom(ctx.atom().getText());
        Constant[] constants = new Constant[atom.getArguments().length];
        for (int i = 0; i < constants.length; i++) {
            constants[i] = (Constant)atom.getArguments()[i];
        }
        float value = visitNumber(ctx.number());

        return new UpdateObservation((StandardPredicate)atom.getPredicate(), constants, value);
    }

    @Override
    public QueryAtom visitQueryAtom(QueryAtomContext ctx) {
        Atom atom = ModelLoader.loadAtom(ctx.atom().getText());
        Constant[] constants = new Constant[atom.getArguments().length];
        for (int i = 0; i < constants.length; i++) {
            constants[i] = (Constant)atom.getArguments()[i];
        }

        return new QueryAtom((StandardPredicate)atom.getPredicate(), constants);
    }

    @Override
    public Stop visitStop(StopContext ctx) {
        return new Stop();
    }

    @Override
    public Sync visitSync(SyncContext ctx) {
        return new Sync();
    }

    @Override
    public WeightLearn visitWeightLearn(WeightLearnContext ctx) {
        return new WeightLearn();
    }

    @Override
    public WriteInferredPredicates visitWriteInferredPredicates(WriteInferredPredicatesContext ctx) {
        String outputDirectoryPath = ctx.STRING_LITERAL().getText().replace("'","").replace("\"", "");
        return new WriteInferredPredicates(outputDirectoryPath);
    }

    @Override
    public Float visitNumber(NumberContext ctx) {
        return Float.parseFloat(ctx.getText());
    }
}