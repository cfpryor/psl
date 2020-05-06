package org.linqs.psl.application.inference.mpe;

import org.junit.Before;
import org.junit.Test;
import org.linqs.psl.TestModel;
import org.linqs.psl.application.inference.InferenceApplication;
import org.linqs.psl.application.inference.InferenceTest;
import org.linqs.psl.database.Database;
import org.linqs.psl.database.DatabaseTestUtil;
import org.linqs.psl.database.atom.AtomManager;
import org.linqs.psl.database.rdbms.driver.DatabaseDriver;
import org.linqs.psl.model.atom.GroundAtom;
import org.linqs.psl.model.atom.ObservedAtom;
import org.linqs.psl.model.atom.QueryAtom;
import org.linqs.psl.model.atom.RandomVariableAtom;
import org.linqs.psl.model.formula.Conjunction;
import org.linqs.psl.model.formula.Implication;
import org.linqs.psl.model.predicate.GroundingOnlyPredicate;
import org.linqs.psl.model.predicate.Predicate;
import org.linqs.psl.model.predicate.StandardPredicate;
import org.linqs.psl.model.rule.GroundRule;
import org.linqs.psl.model.rule.Rule;
import org.linqs.psl.model.rule.arithmetic.expression.coefficient.Add;
import org.linqs.psl.model.rule.logical.WeightedGroundLogicalRule;
import org.linqs.psl.model.rule.logical.WeightedLogicalRule;
import org.linqs.psl.model.term.Constant;
import org.linqs.psl.model.term.ConstantType;
import org.linqs.psl.model.term.UniqueStringID;
import org.linqs.psl.model.term.Variable;
import org.linqs.psl.reasoner.function.FunctionTerm;
import org.linqs.psl.reasoner.function.GeneralFunction;
import org.linqs.psl.reasoner.sgd.term.SGDObjectiveTerm;
import org.linqs.psl.reasoner.sgd.term.SGDStreamingTermStore;
import org.linqs.psl.reasoner.sgd.term.SGDTermGenerator;
import org.linqs.psl.reasoner.term.Hyperplane;
import org.linqs.psl.reasoner.term.HyperplaneTermGenerator;
import org.linqs.psl.server.actions.AddTerm;

import java.util.*;

public class SGDOnlineInferenceTest extends InferenceTest {
    private TestModel.ModelInformation modelInfo;
    private Database inferDB;
    private SGDTermGenerator termGenerator;
    private DatabaseDriver driver = DatabaseTestUtil.getPostgresDriver();
    private Map<String, StandardPredicate> baselinePredicates = new HashMap<>();
    private List<Rule> baselineRules = new ArrayList<Rule>();
    private Map<StandardPredicate, List<TestModel.PredicateData>> baselineObservations = new HashMap<>();
    private Map<StandardPredicate, List<TestModel.PredicateData>> baselineTargets = new HashMap<>();
    private Map<StandardPredicate, List<TestModel.PredicateData>> baselineTruths = new HashMap<>();

    /**
     * Initialize a baseline model that we will be modifying with the online inference application
     */
    private void initBaselineModel() {
        // Define Predicates
        Map<String, ConstantType[]> predicatesInfo = new HashMap<String, ConstantType[]>();
        predicatesInfo.put("Sim_Items", new ConstantType[]{ConstantType.UniqueStringID, ConstantType.UniqueStringID});
        predicatesInfo.put("Sim_Users", new ConstantType[]{ConstantType.UniqueStringID, ConstantType.UniqueStringID});
        predicatesInfo.put("Rating", new ConstantType[]{ConstantType.UniqueStringID, ConstantType.UniqueStringID});
        predicatesInfo.put("Avg_User_Rating", new ConstantType[]{ConstantType.UniqueStringID});

        for (Map.Entry<String, ConstantType[]> predicateEntry : predicatesInfo.entrySet()) {
            StandardPredicate predicate = StandardPredicate.get(predicateEntry.getKey(), predicateEntry.getValue());
            baselinePredicates.put(predicateEntry.getKey(), predicate);
        }

        // Define Rules
        // Rating(U1, M) && Sim_Users(U1, U2) && A != B => Rating(U2, M)
        baselineRules.add(new WeightedLogicalRule(
                new Implication(
                        new Conjunction(
                                new QueryAtom(baselinePredicates.get("Rating"), new Variable("A"), new Variable("M")),
                                new QueryAtom(baselinePredicates.get("Sim_Users"), new Variable("A"), new Variable("B")),
                                new QueryAtom(GroundingOnlyPredicate.NotEqual, new Variable("A"), new Variable("B"))
                        ),
                        new QueryAtom(baselinePredicates.get("Rating"), new Variable("B"), new Variable("M"))
                ),
                1.0,
                true));

        // Rating(U, M1) && Sim_Items(M1, M2) && M1 != M2 => Rating(U, M2)
        baselineRules.add(new WeightedLogicalRule(
                new Implication(
                        new Conjunction(
                                new QueryAtom(baselinePredicates.get("Rating"), new Variable("A"), new Variable("M1")),
                                new QueryAtom(baselinePredicates.get("Sim_Items"), new Variable("M1"), new Variable("M2")),
                                new QueryAtom(GroundingOnlyPredicate.NotEqual, new Variable("M1"), new Variable("M2"))
                        ),
                        new QueryAtom(baselinePredicates.get("Rating"), new Variable("A"), new Variable("M2"))
                ),
                1.0,
                true));

        // Avg_User_Rating(U) => Rating(U,I)
        baselineRules.add(new WeightedLogicalRule(
                new Implication(
                        new Conjunction(
                                new QueryAtom(baselinePredicates.get("Avg_User_Rating"), new Variable("A"))
                        ),
                        new QueryAtom(baselinePredicates.get("Rating"), new Variable("U"), new Variable("M"))
                ),
                1.0,
                true));

        // Data
        // Users: {Alice, Bob, Eddie}
        // Movies: {Titanic, Avatar, Surfs Up}

        // Observed
        // Rating
        baselineObservations.put(baselinePredicates.get("Rating"), new ArrayList<TestModel.PredicateData>(Arrays.asList(
                new TestModel.PredicateData(1.0, new Object[]{"Alice", "Titanic"}),
                new TestModel.PredicateData(0.0, new Object[]{"Alice", "Surfs Up"}),
                new TestModel.PredicateData(0.0, new Object[]{"Bob", "Titanic"}),
                new TestModel.PredicateData(1.0, new Object[]{"Bob", "Avatar"}),
                new TestModel.PredicateData(1.0, new Object[]{"Eddie", "Surfs Up"})
        )));

        // Sim_Users
        baselineObservations.put(baselinePredicates.get("Sim_Users"), new ArrayList<TestModel.PredicateData>(Arrays.asList(
                new TestModel.PredicateData(1.0, new Object[]{"Alice", "Bob"}),
                new TestModel.PredicateData(1.0, new Object[]{"Bob", "Alice"})
        )));

        // Sim_Items
        baselineObservations.put(baselinePredicates.get("Sim_Items"), new ArrayList<TestModel.PredicateData>(Arrays.asList(
                new TestModel.PredicateData(1.0, new Object[]{"Avatar", "Titanic"}),
                new TestModel.PredicateData(1.0, new Object[]{"Titanic", "Avatar"}),
                new TestModel.PredicateData(0.0, new Object[]{"Surfs Up", "Avatar"}),
                new TestModel.PredicateData(0.0, new Object[]{"Avatar", "Surfs Up"}),
                new TestModel.PredicateData(0.0, new Object[]{"Titanic", "Surfs Up"}),
                new TestModel.PredicateData(0.0, new Object[]{"Surfs Up", "Titanic"})
        )));

        // Avg_User_Rating
        baselineObservations.put(baselinePredicates.get("Avg_User_Rating"), new ArrayList<TestModel.PredicateData>(Arrays.asList(
                new TestModel.PredicateData(0.5, new Object[]{"Alice"}),
                new TestModel.PredicateData(0.5, new Object[]{"Bob"}),
                new TestModel.PredicateData(1.0, new Object[]{"Eddie"})
        )));

        // Targets
        // Rating
        baselineTargets.put(baselinePredicates.get("Rating"), new ArrayList<TestModel.PredicateData>(Arrays.asList(
                new TestModel.PredicateData(1.0, new Object[]{"Alice", "Avatar"}),
                new TestModel.PredicateData(0.0, new Object[]{"Bob", "Surfs Up"}),
                new TestModel.PredicateData(1.0, new Object[]{"Eddie", "Avatar"}))
        ));

        // Truths
        baselineTruths.put(baselinePredicates.get("Rating"), new ArrayList<TestModel.PredicateData>(Arrays.asList(
                new TestModel.PredicateData(1.0, new Object[]{"Alice", "Avatar"}),
                new TestModel.PredicateData(1.0, new Object[]{"Bob", "Surfs Up"}),
                new TestModel.PredicateData(1.0, new Object[]{"Eddie", "Avatar"}))
        ));

        modelInfo = TestModel.getModel(driver, baselinePredicates, baselineRules,
                baselineObservations, baselineTargets, baselineTruths);
    }

    @Before
    public void setup() {
        if (inferDB != null) {
            inferDB.close();
            inferDB = null;
        }

        if (modelInfo != null) {
            modelInfo.dataStore.close();
            modelInfo = null;
        }
        
        initBaselineModel();

        // Close the predicates we are using.
        Set<StandardPredicate> toClose = new HashSet<StandardPredicate>();
        toClose.add(modelInfo.predicates.get("Rating"));

        inferDB = modelInfo.dataStore.getDatabase(modelInfo.targetPartition, toClose, modelInfo.observationPartition);

        SGDTermGenerator termGenerator = new SGDTermGenerator();
    }

    @Override
    protected InferenceApplication getInference(List<Rule> rules, Database db) {
        return new SGDOnlineInference(rules, db);
    }

    @Test
    public void testAddTerm(){
        SGDOnlineInference inference = (SGDOnlineInference)getInference(modelInfo.model.getRules(), inferDB);

        SGDStreamingTermStore termStore = (SGDStreamingTermStore)inference.getTermStore();
        AtomManager atomManager = inference.getAtomManager();

        // Create term to add
        // Rating("Alice", "Titanic") && Sim_Users("Eddie", "Alice") => Rating("Eddie", "Titanic")
        GeneralFunction newFTerm = new GeneralFunction(true, true, 1, 1);
        // Might not work because we are creating new constants instead of grabbing existing instances
        RandomVariableAtom rvAtom = (RandomVariableAtom)atomManager.getAtom(Predicate.get("Rating"),
                new UniqueStringID("Eddie"),  new UniqueStringID("Titanic"));
        newFTerm.add(3.0f, rvAtom);

        ObservedAtom obsAtom_1 = (ObservedAtom) atomManager.getAtom(Predicate.get("Rating"),
                new UniqueStringID("Alice"), new UniqueStringID("Titanic"));
        ObservedAtom obsAtom_2 = (ObservedAtom) atomManager.getAtom(Predicate.get("Sim_Users"),
                new UniqueStringID("Eddie"), new UniqueStringID("Alice"));
        newFTerm.add(-1.0f, obsAtom_1);
        newFTerm.add(-1.0f, obsAtom_2);


        SGDObjectiveTerm newTerm = termGenerator.createTerm(newFTerm, (float)1.0, termStore);

        // create new action
        AddTerm newAction = new AddTerm(termStore, newTerm);

        // Set newAction as next action for online inference application
        inference.server.setNextAction(newAction);
    }

}
