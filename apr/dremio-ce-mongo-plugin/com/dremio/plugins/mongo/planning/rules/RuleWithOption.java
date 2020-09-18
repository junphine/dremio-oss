package com.dremio.plugins.mongo.planning.rules;

import org.apache.calcite.tools.*;
import com.dremio.options.*;
import com.google.common.base.*;
import java.util.*;
import org.apache.calcite.plan.*;

public abstract class RuleWithOption extends RelOptRule
{
    public RuleWithOption(final RelOptRuleOperand operand, final RelBuilderFactory relBuilderFactory, final String description) {
        super(operand, relBuilderFactory, description);
    }
    
    public RuleWithOption(final RelOptRuleOperand operand, final String description) {
        super(operand, description);
    }
    
    public RuleWithOption(final RelOptRuleOperand operand) {
        super(operand);
    }
    
    public abstract boolean isEnabled(final OptionManager p0);
    
    public static class OptionPredicate implements Predicate<RuleWithOption>
    {
        private final OptionManager options;
        
        public OptionPredicate(final OptionManager options) {
            this.options = options;
        }
        
        public boolean apply(final RuleWithOption input) {
            return input.isEnabled(this.options);
        }
    }
    
    public static class DelegatingRuleWithOption extends RuleWithOption
    {
        private final RelOptRule delegate;
        private final Predicate<OptionManager> predicate;
        
        public DelegatingRuleWithOption(final RelOptRule delegate, final Predicate<OptionManager> predicate) {
            super(delegate.getOperand(), delegate.relBuilderFactory, delegate.toString());
            this.delegate = delegate;
            this.predicate = predicate;
        }
        
        @Override
        public boolean isEnabled(final OptionManager options) {
            return this.predicate.apply((Object)options);
        }
        
        public void onMatch(final RelOptRuleCall call) {
            this.delegate.onMatch(call);
        }
        
        public RelOptRuleOperand getOperand() {
            return this.delegate.getOperand();
        }
        
        public List<RelOptRuleOperand> getOperands() {
            return (List<RelOptRuleOperand>)this.delegate.getOperands();
        }
        
        public int hashCode() {
            return this.delegate.hashCode();
        }
        
        public boolean equals(final Object obj) {
            return this.delegate.equals(obj);
        }
        
        protected boolean equals(final RelOptRule that) {
            return this.delegate.equals((Object)that);
        }
        
        public boolean matches(final RelOptRuleCall call) {
            return this.delegate.matches(call);
        }
        
        public Convention getOutConvention() {
            return this.delegate.getOutConvention();
        }
        
        public RelTrait getOutTrait() {
            return this.delegate.getOutTrait();
        }
    }
}
