package org.ohnlp.cat.api.criteria;

import org.hl7.fhir.r4.model.DomainResource;
import org.ohnlp.cat.api.cohorts.CandidateScore;

import java.util.*;
import java.util.stream.Collectors;

/**
 * A criterion that applies boolean logics such as MIN_OR/MAX_OR/AND/NOT
 * to 1 or more child criterion depending on the {@link LogicalRelationType}
 * used in {@link #type}
 * <p>
 * For items with numeric modifiers such as MIN_OR and MAX_or, the numeric modifier
 * can be specified via {@link #numericModifier}. Otherwise, the field is ignored.
 */
public class LogicalCriterion extends Criterion {

    private LogicalRelationType type;
    private int numericModifier;
    private List<Criterion> children;

    public LogicalRelationType getType() {
        return type;
    }

    public void setType(LogicalRelationType type) {
        this.type = type;
    }

    public int getNumericModifier() {
        return numericModifier;
    }

    public void setNumericModifier(int numericModifier) {
        this.numericModifier = numericModifier;
    }

    public List<Criterion> getChildren() {
        return children;
    }

    @Override
    public boolean matches(DomainResource resource) {
        switch (type) {
            case AND: {
                for (Criterion c : children) {
                    if (!c.matches(resource)) {
                        return false;
                    }
                }
                return true;
            }
            case MIN_OR: {
                int match = 0;
                for (Criterion val : children) {
                    if (val.matches(resource)) {
                        match++;
                        if (match == numericModifier) {
                            return true;
                        }
                    }
                }
                return false;
            }
            case MAX_OR: {
                int match = 0;
                for (Criterion val : children) {
                    if (val.matches(resource)) {
                        match++;
                        if (match > numericModifier) {
                            return false;
                        }
                    }
                }
                return true;
            }
            case NOT: {
                for (Criterion c : children) {
                    if (c.matches(resource)) {
                        return false;
                    }
                }
                return true;
            }
            default:
                throw new UnsupportedOperationException("Unknown boolean relation type " + type.name());
        }
    }

    @Override
    public double score(Map<UUID, CandidateScore> scoreByCriterionUID) {
        switch (type) {
            case AND: {
                // For AND, simply return average of all subscores
                return children.stream().map(c -> c.score(scoreByCriterionUID)).reduce(Double::sum).orElse(0.00)
                        / ((double)children.size());
            }
            case MIN_OR: {
                // For OR, return max of subscores. For minMatch > 1, use average of top minMatch subscores (thus penalizing if
                // minimum number not met)
                LinkedList<Double> scoresSorted = children.stream().map(c -> c.score(scoreByCriterionUID))
                        .sorted()
                        .collect(Collectors.toCollection(LinkedList::new));
                double scoreTotal = 0;
                int scoresContributedCount = 0;
                while (scoresContributedCount < numericModifier) {
                    scoreTotal += scoresSorted.size() > 0 ? scoresSorted.removeLast() : 0;
                    scoresContributedCount++;
                }
                return scoreTotal / ((double)numericModifier);
            }
            case MAX_OR: {
                // Since this is max_or, we penalize if
                // the number of scores is > numericModifier by subtracting the average
                // of the bottom (|children| - numericModifier) scores from the maximum score
                LinkedList<Double> scoresSorted = children.stream().map(c -> c.score(scoreByCriterionUID))
                        .sorted()
                        .collect(Collectors.toCollection(LinkedList::new));
                double base = scoresSorted.stream().reduce(Math::max).orElse(0.00);
                double penaltyTotal = 0;
                int scoresContributedCount = 0;
                while (scoresContributedCount < numericModifier) {
                    penaltyTotal += scoresSorted.size() > 0 ? scoresSorted.removeFirst() : 0;
                    scoresContributedCount++;
                }
                double penalty = -1.0 * penaltyTotal / ((double)numericModifier);
                return base + penalty;
            }
            case NOT: {
                // For NOT, simply invert the average score of the children (similar to AND scoring)
                return -1 * children.stream().map(c -> c.score(scoreByCriterionUID)).reduce(Double::sum).orElse(0.00)
                        / ((double)children.size());
            }
            default:
                throw new UnsupportedOperationException("Unknown boolean relation type " + type.name());
        }
    }
}
