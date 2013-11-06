package io.spring.reactor.selector;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.context.expression.BeanFactoryResolver;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import reactor.event.selector.ObjectSelector;
import reactor.event.selector.Selector;

/**
 * An implementation of {@link reactor.event.selector.Selector} that uses a SpEL expression to evaluate the match.
 *
 * @author Jon Brisbin
 */
public class ExpressionSelector extends ObjectSelector<Expression> {

	private static final SpelExpressionParser SPEL_PARSER = new SpelExpressionParser();

	private final EvaluationContext evalCtx;

	public ExpressionSelector(Expression expr, EvaluationContext evalCtx) {
		super(expr);
		this.evalCtx = evalCtx;
	}

	@Override
	public boolean matches(Object key) {
		return getObject().getValue(evalCtx, key, Boolean.class);
	}

	/**
	 * Shorthand helper method for creating an {@code ExpressionSelector}.
	 *
	 * @param expr
	 * 		The expression to parse.
	 *
	 * @return A new {@link reactor.event.selector.Selector}
	 */
	public static Selector E(String expr) {
		return expressionSelector(expr, (BeanFactory)null);
	}

	/**
	 * Helper method for creating an {@code ExpressionSelector}.
	 *
	 * @param expr
	 * 		The expression to parse.
	 *
	 * @return A new {@link reactor.event.selector.Selector}
	 */
	public static Selector expressionSelector(String expr) {
		return expressionSelector(expr, (BeanFactory)null);
	}

	/**
	 * Helper method for creating an {@code ExpressionSelector}.
	 *
	 * @param expr
	 * 		The expression to parse.
	 * @param beanFactory
	 * 		The {@link org.springframework.beans.factory.BeanFactory} to use to resolve references in the expression.
	 *
	 * @return A new {@link reactor.event.selector.Selector}
	 */
	public static Selector expressionSelector(String expr, BeanFactory beanFactory) {
		StandardEvaluationContext evalCtx = new StandardEvaluationContext();
		if(null != beanFactory) {
			evalCtx.setBeanResolver(new BeanFactoryResolver(beanFactory));
		}
		return expressionSelector(expr, evalCtx);
	}

	/**
	 * Helper method for creating an {@code ExpressionSelector}.
	 *
	 * @param expr
	 * 		The expression to parse.
	 * @param evalCtx
	 * 		The {@link org.springframework.expression.EvaluationContext} to use.
	 *
	 * @return A new {@link reactor.event.selector.Selector}
	 */
	public static Selector expressionSelector(String expr, EvaluationContext evalCtx) {
		return new ExpressionSelector(SPEL_PARSER.parseExpression(expr), evalCtx);
	}

}
