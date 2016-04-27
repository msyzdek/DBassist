package org.msyzdek.jpa;


import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.function.Function;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.FetchParent;
import javax.persistence.criteria.Path;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;
import javax.persistence.criteria.Selection;
import javax.persistence.TypedQuery;


public abstract class AbstractRepository<T> {

    @FunctionalInterface
    interface SelectFunction<CB, P, S> {
        public S apply(CB criteriaBuilder, P path);
    }

    protected interface OrderBy<T> {
        void apply(CriteriaBuilder criteriaBuilder, CriteriaQuery<?> attributeCriteriaQuery, Root<?> root);
    }

    private final Class<T> typeParameterClass;

    @PersistenceContext
    protected EntityManager entityManager;

    public AbstractRepository(Class<T> typeParameterClass) {
        this.typeParameterClass = typeParameterClass;
    }

    protected <Z, X, A> List<T> find(Conditions conditions, List<Function<FetchParent<?, ?>, FetchParent<?, ?>>> fetchCallbacks, OrderBy<T> orderBy) {
        CriteriaBuilder criteriaBuilder = entityManager.getCriteriaBuilder();
        CriteriaQuery<T> criteriaQuery = criteriaBuilder.createQuery(typeParameterClass);
        Root<T> root = criteriaQuery.from(typeParameterClass);

        applyFetchCallbacks(fetchCallbacks, root);

        criteriaQuery.select(root);

        conditions = applyConditions(conditions, criteriaBuilder, criteriaQuery, root);

        if (orderBy != null) {
            orderBy.apply(criteriaBuilder, criteriaQuery, root);
        }

        TypedQuery<T> typedQuery = entityManager.createQuery(criteriaQuery);
        setParameters(conditions, typedQuery);

        /**
         * Make sure that duplicate query results will be eliminated (when fetching collection relations of the root entity).
         */
        return new ArrayList<T>(new LinkedHashSet<T>(typedQuery.getResultList()));
    }

    protected long count(Conditions conditions) {
        return count(conditions, false);
    }

    protected long countDistinct(Conditions conditions) {
        return count(conditions, true);
    }

    protected <Z, X, A> List<A> findAttribute(Class<A> attributeClass,
                                              String attributeName,
                                              Conditions conditions,
                                              List<Function<FetchParent<?, ?>, FetchParent<?, ?>>> fetchCallbacks,
                                              OrderBy<T> orderBy,
                                              SelectFunction<CriteriaBuilder, Path<A>, Selection<A>> selectCallback) {
        return findAttribute(attributeClass, attributeName, false, conditions, fetchCallbacks, orderBy, selectCallback);
    }

    protected <Z, X, A> List<A> findAttributeDistinct(Class<A> attributeClass,
                                                      String attributeName,
                                                      Conditions conditions,
                                                      List<Function<FetchParent<?, ?>, FetchParent<?, ?>>> fetchCallbacks,
                                                      OrderBy<T> orderBy,
                                                      SelectFunction<CriteriaBuilder,
                                                              Path<A>, Selection<A>> selectCallback) {
        return findAttribute(attributeClass, attributeName, true, conditions, fetchCallbacks, orderBy, selectCallback);
    }

    private <Z, X, A> List<A> findAttribute(Class<A> attributeClass,
                                            String attributeName,
                                            boolean selectDistinct,
                                            Conditions conditions,
                                            List<Function<FetchParent<?, ?>, FetchParent<?, ?>>> fetchCallbacks,
                                            OrderBy<T> orderBy,
                                            SelectFunction<CriteriaBuilder, Path<A>, Selection<A>> selectCallback) {
        CriteriaBuilder criteriaBuilder = entityManager.getCriteriaBuilder();
        CriteriaQuery<A> criteriaQuery = criteriaBuilder.createQuery(attributeClass);
        Root<T> root = criteriaQuery.from(typeParameterClass);

        applyFetchCallbacks(fetchCallbacks, root);

        Selection<? extends A> selection;

        if (selectCallback != null) {
            selection = selectCallback.apply(criteriaBuilder, root.get(attributeName));
        } else {
            selection = root.get(attributeName);
        }

        criteriaQuery.select(selection);

        if (selectDistinct) {
            criteriaQuery.distinct(true);
        }

        conditions = applyConditions(conditions, criteriaBuilder, criteriaQuery, root);

        if (orderBy != null) {
            orderBy.apply(criteriaBuilder, criteriaQuery, root);
        }

        TypedQuery<A> typedQuery = entityManager.createQuery(criteriaQuery);

        setParameters(conditions, typedQuery);

        return typedQuery.getResultList();
    }

    private <X> Conditions applyConditions(Conditions conditions, CriteriaBuilder criteriaBuilder, CriteriaQuery<X> criteriaQuery, Root<T> root) {
        if (conditions == null) {
            return null;
        }

        conditions.apply(criteriaQuery, criteriaBuilder, root);
        return conditions;
    }

    private <X> void setParameters(Conditions conditions, TypedQuery<X> typedQuery) {
        if (conditions != null) {
            conditions.setParameters(typedQuery);
        }
    }

    private void applyFetchCallbacks(List<Function<FetchParent<?, ?>, FetchParent<?, ?>>> fetchCallbacks, Root<T> root) {
        FetchParent<?, ?> rootFetchParent = (FetchParent<?, ?>) root;
        if (fetchCallbacks != null) {
            for (Function<FetchParent<?, ?>, FetchParent<?, ?>> fetchCallback : fetchCallbacks) {
                fetchCallback.apply(rootFetchParent);
            }
        }
    }

    private long count(Conditions conditions, boolean countDistinct) {
        CriteriaBuilder criteriaBuilder = entityManager.getCriteriaBuilder();
        CriteriaQuery<Long> criteriaQuery = criteriaBuilder.createQuery(Long.class);
        Root<T> root = criteriaQuery.from(typeParameterClass);

        if (countDistinct) {
            criteriaQuery.select(criteriaBuilder.countDistinct(root));
        } else {
            criteriaQuery.select(criteriaBuilder.count(root));
        }

        conditions.apply(criteriaQuery, criteriaBuilder, root);

        return (Long) conditions.setParameters(entityManager.createQuery(criteriaQuery)).getSingleResult();
    }

    protected Predicate conjoinPredicates(CriteriaBuilder builder, List<Predicate> predicates) {
        return builder.and(predicates.toArray(new Predicate[predicates.size()]));
    }
}
