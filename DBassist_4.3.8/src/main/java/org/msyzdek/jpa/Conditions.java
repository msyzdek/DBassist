package org.msyzdek.jpa;

import org.hibernate.jpa.criteria.path.AbstractJoinImpl;

import javax.persistence.TypedQuery;
import javax.persistence.criteria.*;
import javax.persistence.metamodel.Attribute;
import java.util.*;


public class Conditions {

    public interface Condition {
        Predicate apply(CriteriaBuilder cb, From<?, ?> root);
    }

    // Per single join
    private HashMap<String, Conditions> joinConditions = new HashMap<String, Conditions>();

    private LinkedList<Condition> whereConditions = new LinkedList<Condition>();

    private HashMap<String, Object> parameters = new HashMap<String, Object>();

    private String joinAttribute;

    private JoinType joinType;

    public Conditions() {}

    private Conditions(String joinAttribute, JoinType joinType) {
        this.joinAttribute = joinAttribute;
        this.joinType = joinType;
    }

    public Condition equal(String attributeName, String value) {
        return addToWhereConditionsAndReturn((cb, root) -> {
            return cb.equal(root.get(attributeName), getExpression(cb, value, String.class));
        });
    }

    public Condition equal(String attributeName, Number value) {
        return addToWhereConditionsAndReturn((cb, root) -> {
            return cb.equal(root.get(attributeName), getExpression(cb, value, Number.class));
        });
    }

    public Condition greaterThan(String attributeName, Date value) {
        return addToWhereConditionsAndReturn((cb, root) -> {
            return cb.greaterThan(root.get(attributeName), getExpression(cb, value, Date.class));
        });
    }

    public Condition greaterThanOrEqualTo(String attributeName, Date value) {
        return addToWhereConditionsAndReturn((cb, root) -> {
            return cb.greaterThanOrEqualTo(root.get(attributeName), getExpression(cb, value, Date.class));
        });
    }

    public Condition lessThan(String attributeName, Date value) {
        return addToWhereConditionsAndReturn((cb, root) -> {
            return cb.lessThan(root.get(attributeName), getExpression(cb, value, Date.class));
        });
    }

    public Condition lessThanOrEqualTo(String attributeName, Date value) {
        return addToWhereConditionsAndReturn((cb, root) -> {
            return cb.lessThanOrEqualTo(root.get(attributeName), getExpression(cb, value, Date.class));
        });
    }

    public Condition in(String attributeName, List<String> values) {
        return addToWhereConditionsAndReturn((cb, root) -> {
            return getInPredicate(attributeName, values, cb, root);
        });
    }

    public Condition notIn(String attributeName, List<String> values) {
        return addToWhereConditionsAndReturn((cb, root) -> {
            return getInPredicate(attributeName, values, cb, root).not();
        });
    }

    public Condition notLike(String attributeName, String value) {
        return addToWhereConditionsAndReturn((cb, root) -> {
            return cb.like(root.get(attributeName), getExpression(cb, value, String.class)).not();
        });
    }

    public Condition isNull(String attributeName) {
        return addToWhereConditionsAndReturn((cb, root) -> {
            return root.get(attributeName).isNull();
        });
    }

    public Condition isNotNull(String attributeName) {
        return addToWhereConditionsAndReturn((cb, root) -> {
            return root.get(attributeName).isNotNull();
        });
    }

    public Condition isBetween(String attributeName, Date leftOperand, Date rightOperand) {
        return addToWhereConditionsAndReturn((cb, root) -> {
            return cb.between(root.get(attributeName), getExpression(cb, leftOperand, Date.class), getExpression(cb, rightOperand, Date.class));
        });
    }

    public Condition or(Condition leftOperandCondition, Conditions joinConditions, Condition rightOperandCondition) {
        if (whereConditions.contains(leftOperandCondition)) {
            whereConditions.remove(leftOperandCondition);
        }

        if (joinConditions.whereConditions.contains(rightOperandCondition)) {
            joinConditions.whereConditions.remove(rightOperandCondition);
        }

        Condition condition = (cb, root) -> {
            From<?, ?> from = getFrom(root, joinConditions);

            return cb.or(leftOperandCondition.apply(cb, root), rightOperandCondition.apply(cb, from));
        };

        return addToWhereConditionsAndReturn(condition);
    }

    private Predicate getInPredicate(String attributeName, List<String> values, CriteriaBuilder cb, From<?, ?> root) {
        return root.get(attributeName).in(getExpression(cb, values, List.class));
    }

    private Condition addToWhereConditionsAndReturn(Condition condition) {
        whereConditions.add(condition);

        return condition;
    }

    public Conditions getJoinConditions(String joinAttribute, JoinType joinType) {
        Conditions conditions = joinConditions.get(joinAttribute);

        if (conditions == null) {
            conditions = new Conditions(joinAttribute, joinType);
            joinConditions.put(joinAttribute, conditions);
        }

        return conditions;
    }

    public void apply(CriteriaQuery<?> criteriaQuery, CriteriaBuilder criteriaBuilder, Root<?> root) {
        applyPredicates(getPredicates(criteriaQuery, criteriaBuilder, root), criteriaQuery, criteriaBuilder);
    }

    public TypedQuery<?> setParameters(TypedQuery<?> typedQuery) {
        parameters.forEach((k, v) -> typedQuery.setParameter(k, v));

        if (joinConditions != null && !joinConditions.isEmpty()) {
            joinConditions.forEach((joinAttribute, joinCondition) -> joinCondition.setParameters(typedQuery));
        }

        return typedQuery;
    }

    private List<Predicate> getPredicates(CriteriaQuery<?> query, CriteriaBuilder cb, From<?, ?> root) {
        List<Predicate> predicates = new LinkedList<Predicate>();

        whereConditions.forEach(condition -> predicates.add(condition.apply(cb, root)));

        if (joinConditions != null && !joinConditions.isEmpty()) {
            joinConditions.forEach((joinAttribute, joinCondition) -> {
                From<?, ?> from = getFrom(root, joinCondition);
                predicates.addAll(joinCondition.getPredicates(query, cb, from));
            });
        }

        return predicates;
    }

    private CriteriaQuery<?> applyPredicates(List<Predicate> predicates, CriteriaQuery<?> query, CriteriaBuilder cb) {
        return query.where(cb.and(predicates.toArray(new Predicate[predicates.size()])));
    }

    private <T> ParameterExpression<T> getExpression(CriteriaBuilder cb, Object value, Class<T> typeParameterClass) {
        String name = getRandomName();
        parameters.put(name, value);

        return cb.parameter(typeParameterClass, name);
    }

    private String getRandomName() {
        return UUID.randomUUID().toString().replaceAll("-", "").replaceAll("\\d", "");
    }

    private From<?, ?> getFrom(From<?, ?> from, Conditions joinCondition) {
        FetchParent<?, ?> fetchParent = null;

        fetchParent = checkExisting(joinCondition, fetchParent, from.getJoins());
        fetchParent = fetchParent != null ? fetchParent : checkExisting(joinCondition, fetchParent, from.getFetches());
        fetchParent = fetchParent != null ? fetchParent : ((From<?, ?>) from).join(joinCondition.joinAttribute, joinCondition.joinType);

        return (From<?, ?>) fetchParent;
    }

    private FetchParent<?, ?> checkExisting(Conditions joinCondition, FetchParent<?, ?> fetchParent, Set<?> joinsOrFetches) {
        if (!joinsOrFetches.isEmpty()) {
            LinkedHashSet<?> existingSingularAttributes = (LinkedHashSet<?>) joinsOrFetches;
            Iterator<?> itor = existingSingularAttributes.iterator();

            while (itor.hasNext()) {
                AbstractJoinImpl<?, ?> existingSingularAttributeJoin = (AbstractJoinImpl<?, ?>) itor.next();
                Attribute<?, ?> joinAttribute = existingSingularAttributeJoin.getAttribute();

                if (joinAttribute.getName().equals(joinCondition.joinAttribute)) {
                    fetchParent = existingSingularAttributeJoin;
                }
            }
        }

        return fetchParent;
    }
}
