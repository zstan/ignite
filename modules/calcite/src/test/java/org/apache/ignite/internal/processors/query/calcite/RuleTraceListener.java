/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptListener;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.FileAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.jetbrains.annotations.Nullable;

/**
 * Records what the planner does with rules and relational nodes during optimization.
 * <p>
 * Every rule attempt, rule production, node registration ({@code relEquivalenceFound}), chosen and discarded node is
 * stored as an {@link Event} in planner order and can be filtered by rule name or by node. The listener is plugged into
 * a planner test through {@code AbstractPlannerTest#physicalPlan(String, IgniteSchema, RelOptListener, String...)} or
 * {@code TestPlanningContextBuilder#planListener(RelOptListener)}:
 * <pre>
 * RuleTraceListener lsnr = RuleTraceListener.forRels(IgniteMergeJoin.class, IgniteHashJoin.class);
 *
 * IgniteRel plan = physicalPlan(sql, schema, lsnr);
 *
 * System.out.println(lsnr.summary()); // how many nodes of which traits each rule / derive step produced.
 * System.out.println(lsnr.dump());    // the full ordered trace.
 * </pre>
 * A node that shows up as a registration without a preceding production of the same rule was produced outside a
 * rule, e.g. by {@code passThrough} / {@code derive} of the top-down driver. To see those driver tasks as well, wrap the
 * planning into {@link #taskTrace(Path)}.
 */
public class RuleTraceListener implements RelOptListener {
    /** Name of the Calcite logger that prints the top-down driver tasks ({@code CalciteTrace#getPlannerTaskTracer()}). */
    public static final String TASK_TRACE_LOGGER = "org.apache.calcite.plan.volcano.task";

    /** Kind of a recorded event. */
    public enum Kind {
        /** A rule matched and was fired (recorded after the call). */
        ATTEMPT,

        /** A rule produced a node (recorded before registration, so the node still carries its own traits). */
        PRODUCE,

        /** A node was registered into a subset, either by a rule or by the driver (passThrough / derive / enforcer). */
        REGISTER,

        /** A node was chosen for the final plan. */
        CHOSEN,

        /** A node was discarded. */
        DISCARD
    }

    /** Single trace entry. */
    public static class Event {
        /** Ordinal number of the event, planner order. */
        public final int seq;

        /** */
        public final Kind kind;

        /** Rule name (see {@link #ruleName(RelOptRule)}), {@code null} for events not caused by a rule. */
        @Nullable public final String rule;

        /** Involved node, {@code null} for an attempt that has no node yet. */
        @Nullable public final RelNode rel;

        /** Textual description captured at the time of the event (nodes are mutated by the planner later on). */
        public final String text;

        /** */
        Event(int seq, Kind kind, @Nullable String rule, @Nullable RelNode rel, String text) {
            this.seq = seq;
            this.kind = kind;
            this.rule = rule;
            this.rel = rel;
            this.text = text;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return String.format("%5d %-8s %-32s %s", seq, kind, rule == null ? "-" : rule, text);
        }
    }

    /** */
    private final Predicate<RelOptRule> ruleFilter;

    /** */
    private final Predicate<RelNode> relFilter;

    /** */
    private final Set<Kind> kinds;

    /** */
    private final List<Event> evts = new ArrayList<>();

    /** */
    private int seq;

    /**
     * @param ruleFilter Rules to record, {@code null} to record all.
     * @param relFilter Nodes to record, {@code null} to record all.
     * @param kinds Event kinds to record.
     */
    public RuleTraceListener(
        @Nullable Predicate<RelOptRule> ruleFilter,
        @Nullable Predicate<RelNode> relFilter,
        Set<Kind> kinds
    ) {
        this.ruleFilter = ruleFilter == null ? r -> true : ruleFilter;
        this.relFilter = relFilter == null ? r -> true : relFilter;
        this.kinds = EnumSet.copyOf(kinds);
    }

    /** Records everything except rule attempts (attempts are numerous and rarely useful). */
    public RuleTraceListener() {
        this(null, null, EnumSet.of(Kind.PRODUCE, Kind.REGISTER, Kind.CHOSEN, Kind.DISCARD));
    }

    /**
     * @param ruleNames Rule names as used by {@code IgnitePlanner#addDisabledRules}, e.g. {@code MergeJoinConverter}
     *                  (the {@code (in:..,out:..)} suffix of converter rules is ignored).
     * @return Listener recording attempts and productions of the given rules only.
     */
    public static RuleTraceListener forRules(String... ruleNames) {
        Set<String> names = new HashSet<>(Arrays.asList(ruleNames));

        return new RuleTraceListener(r -> names.contains(ruleName(r)), null, EnumSet.allOf(Kind.class));
    }

    /**
     * @param rule Rule.
     * @return Rule name without the {@code (in:..,out:..)} suffix that converter rules append, the same form that
     *      {@code IgnitePlanner#addDisabledRules} expects.
     */
    public static String ruleName(RelOptRule rule) {
        String desc = rule.toString();

        int pos = desc.indexOf('(');

        return pos == -1 ? desc : desc.substring(0, pos);
    }

    /**
     * @param relClasses Node classes to trace.
     * @return Listener recording productions, registrations, choices and discards of nodes of the given classes.
     */
    @SafeVarargs
    public static RuleTraceListener forRels(Class<? extends RelNode>... relClasses) {
        return new RuleTraceListener(null, r -> Arrays.stream(relClasses).anyMatch(c -> c.isInstance(r)),
            EnumSet.of(Kind.PRODUCE, Kind.REGISTER, Kind.CHOSEN, Kind.DISCARD));
    }

    /** {@inheritDoc} */
    @Override public void ruleAttempted(RuleAttemptedEvent evt) {
        if (evt.isBefore() || !kinds.contains(Kind.ATTEMPT) || !ruleFilter.test(evt.getRuleCall().getRule()))
            return;

        record(Kind.ATTEMPT, evt.getRuleCall(), evt.getRel(), "on " + describe(evt.getRuleCall().rel(0)));
    }

    /** {@inheritDoc} */
    @Override public void ruleProductionSucceeded(RuleProductionEvent evt) {
        // "before" carries the freshly produced node, "after" carries the same node already registered.
        if (!evt.isBefore() || !kinds.contains(Kind.PRODUCE) || !ruleFilter.test(evt.getRuleCall().getRule()))
            return;

        RelNode rel = evt.getRel();

        if (rel == null || !relFilter.test(rel))
            return;

        record(Kind.PRODUCE, evt.getRuleCall(), rel, "-> " + describe(rel));
    }

    /** {@inheritDoc} */
    @Override public void relEquivalenceFound(RelEquivalenceEvent evt) {
        RelNode rel = evt.getRel();

        if (!kinds.contains(Kind.REGISTER) || rel == null || !relFilter.test(rel))
            return;

        record(Kind.REGISTER, null, rel, describe(rel) + " in " + evt.getEquivalenceClass());
    }

    /** {@inheritDoc} */
    @Override public void relChosen(RelChosenEvent evt) {
        RelNode rel = evt.getRel();

        if (!kinds.contains(Kind.CHOSEN) || rel == null || !relFilter.test(rel))
            return;

        record(Kind.CHOSEN, null, rel, describe(rel));
    }

    /** {@inheritDoc} */
    @Override public void relDiscarded(RelDiscardedEvent evt) {
        RelNode rel = evt.getRel();

        if (!kinds.contains(Kind.DISCARD) || rel == null || !relFilter.test(rel))
            return;

        record(Kind.DISCARD, null, rel, describe(rel));
    }

    /** */
    private void record(Kind kind, @Nullable RelOptRuleCall call, @Nullable RelNode rel, String text) {
        evts.add(new Event(seq++, kind, call == null ? null : ruleName(call.getRule()), rel, text));
    }

    /** @return All recorded events in planner order. */
    public List<Event> events() {
        return Collections.unmodifiableList(evts);
    }

    /**
     * @param kind Kind.
     * @return Recorded events of the given kind.
     */
    public List<Event> events(Kind kind) {
        return evts.stream().filter(e -> e.kind == kind).collect(Collectors.toList());
    }

    /**
     * @param ruleName Rule name, see {@link #ruleName(RelOptRule)}.
     * @return Nodes produced by the given rule.
     */
    public List<RelNode> produced(String ruleName) {
        return evts.stream()
            .filter(e -> e.kind == Kind.PRODUCE && ruleName.equals(e.rule))
            .map(e -> e.rel)
            .collect(Collectors.toList());
    }

    /**
     * @param cls Node class.
     * @return Distinct trait sets the nodes of the given class were registered with, in registration order.
     */
    public List<String> registeredTraits(Class<? extends RelNode> cls) {
        return evts.stream()
            .filter(e -> e.kind == Kind.REGISTER && cls.isInstance(e.rel))
            .map(e -> e.rel.getTraitSet().toString())
            .distinct()
            .collect(Collectors.toList());
    }

    /** Drops all recorded events. */
    public void reset() {
        evts.clear();
        seq = 0;
    }

    /** @return Full trace, one event per line. */
    public String dump() {
        return evts.stream().map(Event::toString).collect(Collectors.joining(System.lineSeparator()));
    }

    /**
     * @return Counts of events grouped by kind, rule and node class with traits, e.g.
     * {@code 4 REGISTER IgniteMergeJoin IGNITE.[0 ASC].affinity[...]}.
     */
    public String summary() {
        Map<String, Integer> cnts = new TreeMap<>();

        for (Event e : evts) {
            String key = e.kind + " " + (e.rule == null ? "-" : e.rule)
                + (e.rel == null ? "" : " " + e.rel.getClass().getSimpleName() + " " + e.rel.getTraitSet());

            cnts.merge(key, 1, Integer::sum);
        }

        return cnts.entrySet().stream()
            .map(en -> String.format("%6d %s", en.getValue(), en.getKey()))
            .collect(Collectors.joining(System.lineSeparator()));
    }

    /**
     * @param file File to write the trace to.
     * @throws IOException If failed.
     */
    public void writeTo(Path file) throws IOException {
        Files.write(file, (summary() + System.lineSeparator() + System.lineSeparator() + dump()).getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Short one-line description of a node: class, id, traits and input subsets. Unlike {@link RelNode#toString()} it does
     * not print the whole subtree.
     *
     * @param rel Node.
     * @return Description.
     */
    public static String describe(RelNode rel) {
        if (rel instanceof RelSubset)
            return rel.toString();

        StringBuilder sb = new StringBuilder(rel.getClass().getSimpleName())
            .append('#').append(rel.getId())
            .append(' ').append(rel.getTraitSet());

        if (!rel.getInputs().isEmpty()) {
            sb.append(" inputs=[");

            for (int i = 0; i < rel.getInputs().size(); i++) {
                RelNode in = rel.getInput(i);

                if (i > 0)
                    sb.append(", ");

                sb.append(in instanceof RelSubset ? in.toString() : in.getClass().getSimpleName() + '#' + in.getId());
            }

            sb.append(']');
        }

        return sb.toString();
    }

    /**
     * Switches on the trace of the Calcite top-down driver ({@code OptimizeGroup}, {@code OptimizeInputs},
     * {@code DeriveTrait}, {@code ApplyRule} tasks and the "Skip ... because of lower bound" pruning messages) and writes it
     * into the given file until the returned handle is closed. Expect 10-20 thousand lines per query.
     * <p>
     * The trace is written through log4j2, so the Ignite test logger must already be initialised (call
     * {@code log()} of the test once before) - otherwise its lazy initialisation re-reads {@code log4j2-test.xml} and drops
     * this logger configuration.
     *
     * @param file Output file, overwritten.
     * @return Handle that stops the trace when closed.
     */
    public static AutoCloseable taskTrace(Path file) {
        LoggerContext ctx = (LoggerContext)LogManager.getContext(false);
        Configuration cfg = ctx.getConfiguration();

        FileAppender app = FileAppender.newBuilder()
            .setName("calcite-task-trace-" + System.nanoTime())
            .withFileName(file.toString())
            .withAppend(false)
            .setLayout(PatternLayout.newBuilder().withPattern("%m%n").build())
            .build();

        app.start();
        cfg.addAppender(app);

        LoggerConfig prev = cfg.getLoggerConfig(TASK_TRACE_LOGGER);
        boolean own = !TASK_TRACE_LOGGER.equals(prev.getName());

        LoggerConfig lc = own ? new LoggerConfig(TASK_TRACE_LOGGER, Level.DEBUG, false) : prev;

        lc.addAppender(app, null, null);
        lc.setLevel(Level.DEBUG);

        if (own)
            cfg.addLogger(TASK_TRACE_LOGGER, lc);

        ctx.updateLoggers();

        return () -> {
            lc.removeAppender(app.getName());

            if (own)
                cfg.removeLogger(TASK_TRACE_LOGGER);

            ctx.updateLoggers();

            app.stop();
        };
    }
}
