package com.dremio.plugins.mongo.planning.rules;

import com.dremio.exec.planner.physical.*;
import com.dremio.exec.*;
import org.apache.calcite.plan.*;
import com.dremio.exec.catalog.*;
import org.apache.calcite.rel.*;
import org.apache.calcite.rex.*;
import org.apache.calcite.rel.type.*;
import com.dremio.plugins.mongo.planning.rels.*;
import java.util.*;
import com.dremio.options.*;

public class MongoProjectRule extends AbstractMongoConverterRule<ProjectPrel>
{
    public static final MongoProjectRule INSTANCE;
    
    public MongoProjectRule() {
        super(ProjectPrel.class, "MongoProjectRule", ExecConstants.MONGO_RULES_PROJECT, true);
    }
    
    @Override
    public MongoRel convert(final RelOptRuleCall call, final ProjectPrel project, final StoragePluginId pluginId, final RelNode input) {
        for (int projectFieldIndex = 0; projectFieldIndex < project.getProjects().size(); ++projectFieldIndex) {
            final RexNode rexNode = project.getProjects().get(projectFieldIndex);
            if (!(rexNode instanceof RexInputRef)) {
                return null;
            }
            final RexInputRef inputRef = (RexInputRef)rexNode;
            final int indexOfChild = inputRef.getIndex();
            final RelDataTypeField fieldInInput = project.getInput().getRowType().getFieldList().get(indexOfChild);
            if (fieldInInput.getName().endsWith("*") || !project.getRowType().getFieldList().get(projectFieldIndex).equals(fieldInInput)) {
                return null;
            }
        }
        return new MongoProject(AbstractMongoConverterRule.withMongo((RelNode)project), input, project.getProjects(), project.getRowType(), CollationFilterChecker.hasCollationFilter(project.getInput()));
    }
    
    static {
        INSTANCE = new MongoProjectRule();
    }
}
