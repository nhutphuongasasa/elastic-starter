package com.example.elasticsearch_spring_starter.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.example.elasticsearch_spring_starter.response.PageResult;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.SortOptions;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.query_dsl.MatchAllQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch._types.query_dsl.QueryStringQuery;
import co.elastic.clients.elasticsearch.core.MsearchRequest;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.search.SourceConfig;
import org.apache.commons.beanutils.PropertyUtils;
import org.springframework.data.elasticsearch.UncategorizedElasticsearchException;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class SearchBuilder {
    private static final String HIGHLIGHTER_PRE_TAGS = "<mark>";
    private static final String HIGHLIGHTER_POST_TAGS = "</mark>";    

    private static final String SORT_ORDER_ASC = "asc";

    private ElasticsearchClient client;
    private String[] indices;
    private Query query;
    private List<SortOptions> sorts = new ArrayList<>();
    private int from = 0;
    private int size = 10;
    private boolean trackTotalHits = false;
    private String highlightField;
    private String[] routing;

    private SearchBuilder(
        ElasticsearchClient elasticsearchClient,
        String... indices
    ){
        this.client = elasticsearchClient;
        this.indices = indices;
    }

    public static SearchBuilder builder(
        ElasticsearchClient elasticsearchClient,
        String... indices
    ){
        return new SearchBuilder(elasticsearchClient, indices);
    }

    public SearchBuilder setIndices(String... indices){
        if(indices.length > 0 && indices != null){
            this.indices = indices;
        }
        return this;
    }

    public SearchBuilder setStringQuery(String queryStr){
        if(queryStr != null){
            this.query = Query.of(q -> q.queryString(
                QueryStringQuery.of(
                    qs -> qs.query(queryStr)
                )
            ));
        } else{
            this.query = Query.of(
                q -> q.matchAll(
                    MatchAllQuery.of(m -> m)
                )
            );
        }

        return this;
    }

    public SearchBuilder setPage(Integer page, Integer limit, boolean trackTotalHits){
        if(page != null && limit != null){
            this.from = (page -1) * limit;

            this.size = limit;

            this.trackTotalHits = trackTotalHits;
        }

        return this;
    }

    public SearchBuilder setPage(Integer page, Integer limit){
        return setPage(page, limit, false);
    }

    public SearchBuilder addSort(String field, String order){
        if(field != null && order != null){
            SortOrder so = SORT_ORDER_ASC.equals(order) ? SortOrder.Asc : SortOrder.Desc;
            sorts.add(
                SortOptions.of(
                    s -> s.field(
                        f -> f.field(field).order(so)
                    )
                )
            );
        }

        return this;
    }

    public SearchBuilder setHighlight(String field, String preTags, String postTags){
        if(field != null && preTags != null && postTags != null){
            this.highlightField = field;
        }
        return this;
    }

    public SearchBuilder setHighlightField(String highlightField){
        if(highlightField != null){
            this.setHighlight("*", HIGHLIGHTER_PRE_TAGS, HIGHLIGHTER_POST_TAGS);
        }
        return this;
    }

    public SearchBuilder setRouting(String... routing){
        if(routing != null){
            this.routing = routing;
        }

        return this;
    }

    public SearchResponse<JsonNode> get() throws IOException{
        SearchRequest.Builder builder = new SearchRequest.Builder()
            .query(query)
            .from(from)
            .size(size)
            .trackScores(trackTotalHits)//kiiem tra so luong 
            .source(SourceConfig.of(s -> s.filter(f -> f.includes("*"))));

        if(indices != null && indices.length > 0){
            builder.index(Arrays.asList(indices));
        }

        if(highlightField != null){
            builder.highlight(h -> h.fields(highlightField,
                f -> f.preTags(HIGHLIGHTER_PRE_TAGS)
                    .postTags(HIGHLIGHTER_POST_TAGS)
            ));
        }

        if(sorts != null && sorts.size() > 0){
            builder.sort(sorts);
        }

        if (routing != null && routing.length > 0) {
            builder.routing(String.join(",", routing));
        }

        SearchResponse<JsonNode> result = client.search(builder.build(), JsonNode.class);

        System.out.println(result);

        return result;
    }


    public List<Hit<JsonNode>> getHits() throws IOException {
        List<Hit<JsonNode>> allHits = new ArrayList<>();

        // Trường hợp có nhiều routing → dùng Msearch
        if (routing != null && routing.length > 1) {
            MsearchRequest.Builder msearchBuilder = new MsearchRequest.Builder();

            for (String route : routing) {
                SearchRequest.Builder singleRequest = new SearchRequest.Builder()
                    .index(Arrays.asList(indices))
                    .query(query)
                    .from(from)
                    .size(size)
                    .trackScores(trackTotalHits)
                    .source(s -> s.filter(f -> f.includes("*")));

                if (highlightField != null) {
                    singleRequest.highlight(h -> h.fields(highlightField,
                        f -> f.preTags(HIGHLIGHTER_PRE_TAGS).postTags(HIGHLIGHTER_POST_TAGS)));
                }

                if (sorts != null && !sorts.isEmpty()) {
                    singleRequest.sort(sorts);
                }

                // Add to MsearchRequest
                msearchBuilder.searches(s -> s
                    .header(h -> h.index(Arrays.asList(indices)).routing(route))
                    .body(b -> b
                        .query(query)
                        .from(from)
                        .size(size)
                        .trackScores(trackTotalHits)
                        .source(src -> src.filter(f -> f.includes("*")))
                        .highlight(highlightField != null ?
                            h -> h.fields(highlightField,
                                f -> f.preTags(HIGHLIGHTER_PRE_TAGS).postTags(HIGHLIGHTER_POST_TAGS)) :
                            null)
                        .sort(sorts != null ? sorts : new ArrayList<>())
                    )
                );
            }

            var msearchResponse = client.msearch(msearchBuilder.build(), JsonNode.class);

            for (var item : msearchResponse.responses()) {
                if (item.result() != null) {
                    allHits.addAll(item.result().hits().hits());
                } else if (item.isFailure()) {
                    System.err.println("Search error: " + item.failure().error().reason());
                }
            }
        } 

        return allHits;
    }

    private String concat(List<String> fragments) {
        return fragments.stream().collect(Collectors.joining());
    }


    public <T> void populateHighLightedFields(
        T result,
        Map<String, List<String>> highlightFields
    ){
        for (Map.Entry<String, List<String>> field : highlightFields.entrySet()){
            try {
                String name = field.getKey();

                if(!name.endsWith(".keyword")){
                    if(result instanceof ObjectNode){
                        ((ObjectNode) result).put(name, concat(field.getValue()));
                    } else{
                        PropertyUtils.setProperty(result, name, concat(field.getValue()));
                    }
                }
            } catch (Exception e) {
                throw new UncategorizedElasticsearchException("failed to set highlighted value for field: " + field.getKey()
                        + " with value: " + field.getValue(), e);
            }
        }
    }

    public List<JsonNode> getList(List<Hit<JsonNode>> hits) throws IOException{
        List<JsonNode> list = new ArrayList<>();
        
        if(hits != null && hits.size() > 0){
            for(Hit<JsonNode> hit : hits){
                JsonNode jsonNode = hit.source();

                ObjectNode objectNode =  (ObjectNode) jsonNode;
                
                if(objectNode != null){
                    objectNode.put("id", hit.id());

                    Map<String, List<String>> highlightFields = hit.highlight();

                    if(highlightFields != null && highlightFields.size() > 0){
                        populateHighLightedFields(objectNode, highlightFields);
                    }

                    list.add(objectNode);
                }
            }
        }

        return list;
    }

    public List<JsonNode> getList() throws IOException{
        return getList(this.get().hits().hits());
    }

    public PageResult<JsonNode> getPage(
        Integer page,
        Integer limit
    ) throws IOException{
        this.setPage(page, limit);

        List<JsonNode> response = this.getList();

        long totalCnt = (response.isEmpty() && response == null) ? 0L : response.size();

        return PageResult.<JsonNode>builder()
            .data(response)
            .code(0)
            .count(totalCnt)
            .build();
    }

    public PageResult<JsonNode> getPage() throws IOException{
        return this.getPage(null, null);
    }


}