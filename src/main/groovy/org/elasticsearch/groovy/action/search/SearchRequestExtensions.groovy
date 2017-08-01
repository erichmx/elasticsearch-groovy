/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.groovy.action.search

import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.action.search.SearchRequestBuilder
import org.elasticsearch.client.Client
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.common.io.stream.InputStreamStreamInput
import org.elasticsearch.common.bytes.BytesArray
import org.elasticsearch.common.bytes.BytesReference
import org.elasticsearch.search.SearchModule
import org.elasticsearch.search.SearchRequestParsers
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.index.query.QueryParseContext
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory
import org.elasticsearch.common.ParseFieldMatcher;


/**
 * {@code SearchRequestExtensions} provides Groovy-friendly {@link SearchRequest} extensions.
 * @see Client#search(SearchRequest)
 */
class SearchRequestExtensions {
    /**
     * Sets the content query {@code source}.
     *
     * @param self The {@code this} reference for the {@link SearchRequest}.
     * @param source The content source
     * @return Always {@code self}.
     * @throws NullPointerException if any parameter is {@code null}
     */
    static SearchRequest source(SearchRequest self, Closure source) {
//        self.source(source.asJsonBytes())
        self.source(closureToSourceBuilder(source))
    }

    private static SearchModule searchModule = new SearchModule(Settings.EMPTY, false, Collections.emptyList());
    private static SearchSourceBuilder closureToSourceBuilder(Closure source){
        SearchRequestParsers searchRequestParsers = searchModule.searchRequestParsers;
        BytesReference searchSourceAsBytes = new BytesArray(source.asJsonBytes())
        XContentParser parser = XContentFactory.xContent(searchSourceAsBytes).createParser(searchSourceAsBytes);
        QueryParseContext parseContext = new QueryParseContext(searchRequestParsers.queryParsers, parser, ParseFieldMatcher.STRICT);
        SearchSourceBuilder newBuilder = SearchSourceBuilder.fromXContent(parseContext, searchRequestParsers.aggParsers,
          searchRequestParsers.suggesters, searchRequestParsers.searchExtParsers);

        return newBuilder
    }


    /**
     * Sets theextra  content query {@code source}.
     *
     * @param self The {@code this} reference for the {@link SearchRequest}.
     * @param extraSource The extra content source
     * @return Always {@code self}.
     */
    static SearchRequest extraSource(SearchRequest self, Closure extraSource) {
        self.extraSource(extraSource.asJsonBytes())
    }

    /**
     * Sets the content query {@code source}.
     *
     * @param self The {@code this} reference for the {@link SearchRequestBuilder}.
     * @param source The content source
     * @return Always {@code self}.
     * @throws NullPointerException if any parameter is {@code null}
     */
    static SearchRequestBuilder setSource(SearchRequestBuilder self, Closure source) {
        self.setSource(closureToSourceBuilder(source))
    }

    /**
     * Sets the extra content query {@code source}.
     *
     * @param self The {@code this} reference for the {@link SearchRequestBuilder}.
     * @param extraSource The extra content source
     * @return Always {@code self}.
     * @throws NullPointerException if any parameter is {@code null}
     */
    static SearchRequestBuilder setExtraSource(SearchRequestBuilder self, Closure extraSource) {
        self.setExtraSource(extraSource.asJsonBytes())
    }

    /**
     * Sets a filter on the query executed that only applies to the search query (and not facets for example).
     *
     * @param self The {@code this} reference for the {@link SearchRequestBuilder}.
     * @param postFilter The post filter
     * @return Always {@code self}.
     * @throws NullPointerException if any parameter is {@code null}
     */
    static SearchRequestBuilder setPostFilter(SearchRequestBuilder self, Closure postFilter) {
        self.setPostFilter(postFilter.asJsonBytes())
    }

    /**
     * Constructs a new search source builder with a raw search query.
     * <p>
     * Note: When building a new {@link SearchRequest}, using this method will overwrite other changes related to the
     * query.
     *
     * @param self The {@code this} reference for the {@link SearchRequestBuilder}.
     * @param query The search query
     * @return Always {@code self}.
     * @throws NullPointerException if any parameter is {@code null}
     */
    static SearchRequestBuilder setQuery(SearchRequestBuilder self, Closure query) {
        self.setQuery(query.asJsonBytes())
    }

    /**
     * Sets the aggregations to perform as part of the search.
     *
     * @param self The {@code this} reference for the {@link SearchRequestBuilder}.
     * @param aggregations The aggregations
     * @return Always {@code self}.
     * @throws NullPointerException if any parameter is {@code null}
     */
    static SearchRequestBuilder setAggregations(SearchRequestBuilder self, Closure aggregations) {
        self.setAggregations(aggregations.asJsonBytes())
    }
}