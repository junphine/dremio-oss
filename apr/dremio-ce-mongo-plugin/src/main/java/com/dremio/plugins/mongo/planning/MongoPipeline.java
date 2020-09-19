package com.dremio.plugins.mongo.planning;

import com.fasterxml.jackson.annotation.*;
import com.google.common.collect.*;
import org.bson.*;
import org.bson.conversions.*;
import com.mongodb.client.model.*;
import com.mongodb.client.*;
import java.util.*;
import org.slf4j.*;

public class MongoPipeline
{
    private static final Logger logger;
    private final Document match;
    private final Document project;
    private final boolean needsCollation;
    
    public MongoPipeline(@JsonProperty("match") final String match, @JsonProperty("project") final String project, @JsonProperty("needsCollation") final boolean needsCollation) {
        this((match == null) ? null : Document.parse(match), (project == null) ? null : Document.parse(project), needsCollation);
    }
    
    public MongoPipeline(final Document match, final Document project, final boolean needsCollation) {
        this.match = match;
        this.project = project;
        this.needsCollation = needsCollation;
    }
    
    public static MongoPipeline createMongoPipeline(final List<Document> pipelinesInput, final boolean needsCollation) {
        if (pipelinesInput == null || pipelinesInput.isEmpty()) {
            return new MongoPipeline(null, (Document)null, needsCollation);
        }
        if (pipelinesInput.size() == 1) {
            final Document doc = pipelinesInput.get(0);
            final Document project = getTrivialProject(doc);
            final Document match = getMatch(doc);
            if (match != null) {
                return new MongoPipeline(match, null, needsCollation);
            }
            if (project != null) {
                final Document finalProject = project.isEmpty() ? null : project;
                return new MongoPipeline(null, finalProject, needsCollation);
            }
        }
        else if (pipelinesInput.size() == 2) {
            final Document firstDoc = pipelinesInput.get(0);
            final Document secondDoc = pipelinesInput.get(1);
            if (firstDoc != null && secondDoc != null) {
                final Document projFromDoc1 = getTrivialProject(firstDoc);
                final Document matchFromDoc2 = getMatch(secondDoc);
                final Document matchFromDoc3 = getMatch(firstDoc);
                final Document projFromDoc2 = getTrivialProject(secondDoc);
                if (projFromDoc1 != null && matchFromDoc2 != null) {
                    return new MongoPipeline(matchFromDoc2, projFromDoc1, needsCollation);
                }
                if (matchFromDoc3 != null && projFromDoc2 != null) {
                    return new MongoPipeline(matchFromDoc3, projFromDoc2, needsCollation);
                }
                if (projFromDoc1 != null && projFromDoc2 != null) {
                    return new MongoPipeline(null, projFromDoc2, needsCollation);
                }
            }
        }
        else if (pipelinesInput.size() == 3) {
            final Document projFromDoc3 = getTrivialProject(pipelinesInput.get(0));
            final Document matchFromDoc4 = getMatch(pipelinesInput.get(1));
            final Document projFromDoc4 = getTrivialProject(pipelinesInput.get(2));
            final Document matchFromDoc5 = getMatch(pipelinesInput.get(0));
            final Document projFromDoc5 = getTrivialProject(pipelinesInput.get(1));
            final Document projFromDoc6 = getTrivialProject(pipelinesInput.get(2));
            if (projFromDoc3 != null && matchFromDoc4 != null && projFromDoc4 != null && isTrivialProject(projFromDoc3) && isTrivialProject(projFromDoc4)) {
                return new MongoPipeline(matchFromDoc4, projFromDoc4, needsCollation);
            }
            if (matchFromDoc5 != null && projFromDoc5 != null && projFromDoc6 != null) {
                return new MongoPipeline(matchFromDoc5, projFromDoc6, needsCollation);
            }
        }
        throw new IllegalStateException(String.format("Mongo aggregation framework support has been removed. Number of pipelines: %d.", pipelinesInput.size()));
    }
    
    @JsonProperty
    public boolean needsCollation() {
        return this.needsCollation;
    }
    
    @JsonProperty
    public String getProject() {
        return (this.project == null) ? null : this.project.toJson();
    }
    
    @JsonIgnore
    public Document getProjectAsDocument() {
        return this.project;
    }
    
    @JsonProperty
    public String getMatch() {
        return (this.match == null) ? null : this.match.toJson();
    }
    
    @JsonIgnore
    public Document getMatchAsDocument() {
        return this.match;
    }
    
    @JsonIgnore
    public List<Document> getPipelines() {
        final List<Document> pipelineToReturn = Lists.newArrayListWithCapacity(2);
        if (this.match != null) {
            pipelineToReturn.add(new Document(MongoPipelineOperators.MATCH.getOperator(), this.match));
        }
        if (this.project != null) {
            pipelineToReturn.add(new Document(MongoPipelineOperators.PROJECT.getOperator(), this.project));
        }
        return pipelineToReturn;
    }
    
    @JsonIgnore
    public MongoPipeline applyMinMaxFilter(final Document minFilters, final Document maxFilters) {
        final Map<String, List<Document>> rangeFilter = MongoFilterMerger.mergeFilters(minFilters, maxFilters);
        if (rangeFilter == null || rangeFilter.size() == 0) {
            return this;
        }
        final Document newMatch = buildFilters(this.match, rangeFilter);
        return new MongoPipeline(newMatch, this.getProjectAsDocument(), this.needsCollation());
    }
    
    @JsonIgnore
    public boolean hasProject() {
        return this.getPipelines().stream().anyMatch(p -> p.get(MongoPipelineOperators.PROJECT.getOperator()) != null);
    }
    
    @JsonIgnore
    public MongoCursor<RawBsonDocument> getCursor(final MongoCollection<RawBsonDocument> collection, final int targetRecordCount) {
        MongoPipeline.logger.debug("Filters Applied : " + this.match);
        MongoPipeline.logger.debug("Fields Selected :" + this.project);
        FindIterable<RawBsonDocument> cursor;
        if (this.match == null) {
            cursor = (FindIterable<RawBsonDocument>)collection.find();
        }
        else {
            cursor = (FindIterable<RawBsonDocument>)collection.find((Bson)this.match);
        }
        if (this.project != null) {
            cursor = (FindIterable<RawBsonDocument>)cursor.projection((Bson)this.project);
        }
        if (this.needsCollation) {
            cursor = (FindIterable<RawBsonDocument>)cursor.collation(Collation.builder().locale("en_US").numericOrdering(true).build());
        }
        return (MongoCursor<RawBsonDocument>)cursor.batchSize(targetRecordCount).iterator();
    }
    
    public static Document getTrivialProject(final Document pipelineEntry) {
        final Document proj = (Document)pipelineEntry.get(MongoPipelineOperators.PROJECT.getOperator());
        if (proj == null || !isTrivialProject(proj)) {
            return null;
        }
        return proj;
    }
    
    private static Document getMatch(final Document pipelineEntry) {
        return (Document)pipelineEntry.get(MongoPipelineOperators.MATCH.getOperator());
    }
    
    @JsonIgnore
    public boolean isOnlyTrivialProjectOrFilter() {
        return !this.hasProject() || isTrivialProject(this.project);
    }
    
    @JsonIgnore
    public boolean isSimpleScan() {
        return !this.hasProject();
    }
    
    public boolean needsCoercion() {
        for (final Document pipe : this.getPipelines()) {
            if (pipe.get(MongoPipelineOperators.UNWIND.getOperator()) != null) {
                return true;
            }
            final Document projectDoc = (Document)pipe.get(MongoPipelineOperators.PROJECT.getOperator());
            if (projectDoc != null && !isTrivialProject(projectDoc)) {
                return true;
            }
        }
        return false;
    }
    
    public static boolean isTrivialProject(final Document projectDoc) {
        for (final Map.Entry<String, Object> entry : projectDoc.entrySet()) {
            final Object valueObj = entry.getValue();
            if (valueObj instanceof Integer) {
                if ((int)valueObj != 1) {
                    return false;
                }
                continue;
            }
            else {
                if (!(valueObj instanceof Boolean)) {
                    return false;
                }
                if (!(boolean)valueObj) {
                    return false;
                }
                continue;
            }
        }
        return true;
    }
    
    private static Document buildFilters(final Document pushdownFilters, final Map<String, List<Document>> mergedFilters) {
        Document toReturn = null;
        final List<Document> listToAnd = new ArrayList<Document>();
        for (final Map.Entry<String, List<Document>> entry : mergedFilters.entrySet()) {
            final List<Document> list = entry.getValue();
            assert list.size() == 2 : "Chunk min/max filter should be of size 1 or 2, but got size " + list.size();
            if (list.size() == 1) {
                listToAnd.addAll(list);
            }
            else {
                final String fieldName = entry.getKey();
                final Document rangeQuery = new Document();
                Document bound = (Document)list.get(0).get(fieldName);
                rangeQuery.putAll((Map)bound);
                bound = (Document)list.get(1).get(fieldName);
                rangeQuery.putAll((Map)bound);
                listToAnd.add(new Document(fieldName, rangeQuery));
            }
        }
        if (pushdownFilters != null && !pushdownFilters.isEmpty()) {
            listToAnd.add(pushdownFilters);
        }
        if (listToAnd.size() > 0) {
            final Document andQueryFilter = new Document();
            andQueryFilter.put(MongoFunctions.AND.getMongoOperator(), listToAnd);
            toReturn = andQueryFilter;
        }
        return toReturn;
    }
    
    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("find(");
        sb.append((this.match == null) ? "{}" : this.match.toJson());
        sb.append(", ");
        sb.append((this.project == null) ? "{}" : this.project.toJson());
        sb.append(")");
        return sb.toString();
    }
    
    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof MongoPipeline)) {
            return false;
        }
        final MongoPipeline that = (MongoPipeline)other;
        return Objects.equals(this.match, that.match) && Objects.equals(this.project, that.project);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(this.match, this.project);
    }
    
    public MongoPipeline copy() {
        return new MongoPipeline(this.match, this.project, this.needsCollation);
    }
    
    public MongoPipeline newWithoutProject() {
        return new MongoPipeline(this.match, null, this.needsCollation);
    }
    
    static {
        logger = LoggerFactory.getLogger(MongoPipeline.class);
    }
}
