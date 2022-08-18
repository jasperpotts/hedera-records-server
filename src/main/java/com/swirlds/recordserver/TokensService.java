package com.swirlds.recordserver;

import com.swirlds.recordserver.util.QueryParamUtil;
import io.helidon.config.Config;
import io.helidon.metrics.api.RegistryFactory;
import io.helidon.webserver.Routing;
import io.helidon.webserver.ServerRequest;
import io.helidon.webserver.ServerResponse;
import io.helidon.webserver.Service;
import jakarta.json.Json;
import jakarta.json.JsonArrayBuilder;
import jakarta.json.JsonBuilderFactory;
import jakarta.json.JsonObject;
import jakarta.json.JsonObjectBuilder;
import org.apache.pinot.client.Connection;
import org.apache.pinot.client.ConnectionFactory;
import org.apache.pinot.client.PreparedStatement;
import org.apache.pinot.client.Request;
import org.apache.pinot.client.ResultSet;
import org.apache.pinot.client.ResultSetGroup;
import org.eclipse.microprofile.metrics.Counter;
import org.eclipse.microprofile.metrics.MetricRegistry;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.logging.Logger;

import static com.swirlds.recordserver.util.QueryParamUtil.parseLimitQueryString;
import static com.swirlds.recordserver.util.Utils.parseFromColumn;

/**
 * A service for tokens API
 */
public class TokensService implements Service {

    private static final Logger LOGGER = Logger.getLogger(TokensService.class.getName());
    private static final JsonBuilderFactory JSON = Json.createBuilderFactory(Collections.emptyMap());
    private final MetricRegistry registry = RegistryFactory.getInstance().getRegistry(MetricRegistry.Type.APPLICATION);
    private final Counter accessCtr = registry.counter("accessctr");
    private final Connection pinotConnection;

    TokensService(Config config) {
        this.pinotConnection = ConnectionFactory.fromHostList(config.get("pinot-broker").asString().orElse("pinot-broker:8099"));
    }

    /**
     * A service registers itself by updating the routing rules.
     *
     * @param rules the routing rules.
     */
    @Override
    public void update(Routing.Rules rules) {
        rules.get("/", this::getDefaultMessageHandler);
    }

    private void getDefaultMessageHandler(ServerRequest request, ServerResponse response) {
        final Optional<String> publicKeyQueryParam = request.queryParams().first("publickey");
        final Optional<String> tokenIdQueryParam = request.queryParams().first("token.id");
        final Optional<String> tokenTypesQueryParam = request.queryParams().first("type"); // TODO (MYK): can be a list
        final Optional<String> limitParam = request.queryParams().first("limit");
        final Optional<String> accountIdQueryParam = request.queryParams().first("account.id");
        final Optional<String> orderParam = request.queryParams().first("order");

        // build and execute query
        final List<QueryParamUtil.WhereClause> whereClauses = new ArrayList<>();
        // always restrict query (over all entities) to only return tokens
        whereClauses.add(new QueryParamUtil.WhereClause(QueryParamUtil.Type._string, "type",
                QueryParamUtil.Comparator.eq, "TOKEN"));
        publicKeyQueryParam.ifPresent(s ->
                whereClauses.add(QueryParamUtil.parseQueryString(QueryParamUtil.Type._string, "public_key", s)));
        tokenIdQueryParam.ifPresent(s ->
                whereClauses.add(QueryParamUtil.parseQueryString(QueryParamUtil.Type._long, "entity_number", s)));
        tokenTypesQueryParam.ifPresent(s -> {
                if (!s.equalsIgnoreCase("ALL")) {
                    whereClauses.add(QueryParamUtil.parseQueryString(QueryParamUtil.Type._string,
                            "JSON_EXTRACT_SCALAR(fields, '$.tokenType', 'STRING', 'null')", s));
		}
        });
        accountIdQueryParam.ifPresent(s ->
                whereClauses.add(QueryParamUtil.parseQueryString(QueryParamUtil.Type._long,
                        "JSON_EXTRACT_SCALAR(fields, '$.autoRenewAccount', 'STRING', '0.0.0')", s)));

        final String whereClause = whereClauses.isEmpty() ? "" : "where " +
                QueryParamUtil.whereClausesToQuery(whereClauses);
        final String direction = QueryParamUtil.Order.parse(orderParam).toString();
        final int limit = parseLimitQueryString(limitParam); // limit
        final String queryString =
                "select consensus_timestamp, entity_number, evm_address, alias, public_key_type, public_key, fields " +
                "from entity " + whereClause + " order by consensus_timestamp " + direction + " limit ?";
        System.out.println("queryString = " + queryString);
        final PreparedStatement statement = this.pinotConnection.prepareStatement(new Request("sql",queryString));
        QueryParamUtil.applyWhereClausesToQuery(whereClauses, statement);
        statement.setInt(whereClauses.size(), limit);
        final ResultSetGroup pinotResultSetGroup = statement.execute();
        final ResultSet resultTableResultSet = pinotResultSetGroup.getResultSet(0);

        // format results to JSON
        long latestConsensusTime = (direction.equalsIgnoreCase("asc") ? 0 : Long.MAX_VALUE);
        final JsonArrayBuilder tokensArray = JSON.createArrayBuilder();
        for (int i = 0; i < resultTableResultSet.getRowCount(); i++) {
            final long consensusTime = resultTableResultSet.getLong(i, 0);
            latestConsensusTime = (direction.equalsIgnoreCase("asc") ? Math.max(latestConsensusTime, consensusTime) :
                    Math.min(latestConsensusTime, consensusTime));
            final JsonObject fields = parseFromColumn(resultTableResultSet.getString(i, 6));
            tokensArray.add(JSON.createObjectBuilder()
                    .add("alias", resultTableResultSet.getString(i, 3))
                    .add("consensus_timestamp", consensusTime)
                    .add("entity_number", resultTableResultSet.getLong(i, 1))
                    .add("evm_address", resultTableResultSet.getString(i, 2))
                    .add("public_key", resultTableResultSet.getString(i, 5))
                    .add("public_key_type", resultTableResultSet.getString(i, 4))
                    .add("realm", fields.getString("realm", ""))
                    .add("shard", fields.getString("shard", ""))
                    .add("name", fields.getString("name", ""))
                    .add("symbol", fields.getString("symbol", ""))
                    .add("decimals", fields.getString("decimals", ""))
                    .add("initialSupply", fields.getString("initialSupply", ""))
                    .add("treasury", fields.getString("treasury", ""))
                    .add("adminKey", fields.getString("adminKey", ""))
                    .add("kycKey", fields.getString("kycKey", ""))
                    .add("freezeKey", fields.getString("freezeKey", ""))
                    .add("wipeKey", fields.getString("wipeKey", ""))
                    .add("supplyKey", fields.getString("supplyKey", ""))
                    .add("pauseKey", fields.getString("pauseKey", ""))
                    .add("freezeDefault", fields.getString("freezeDefault", ""))
                    .add("expiry", fields.getString("expiry", ""))
                    .add("autoRenewAccount", fields.getString("autoRenewAccount", ""))
                    .add("autoRenewPeriod", fields.getString("autoRenewPeriod", ""))
                    .add("memo", fields.getString("memo", ""))
                    .add("tokenType", fields.getString("tokenType", ""))
                    .add("supplyType", fields.getString("supplyType", ""))
                    .add("maxSupply", fields.getString("maxSupply", ""))
                    .add("feeScheduleKey", fields.getString("feeScheduleKey", ""))
                    .build());
        }
        final JsonObjectBuilder returnObject = JSON.createObjectBuilder()
                .add("tokens", tokensArray.build())
                .add("links", JSON.createObjectBuilder()
                        .add("next","/api/v1/tokens?order=" + direction + "&limit=" + limit + "&consensus_timestamp=" +
                                (direction.equalsIgnoreCase("asc") ? "gt" : "lt") + latestConsensusTime)
                        .build());
        response.send(returnObject.build());
    }
}
