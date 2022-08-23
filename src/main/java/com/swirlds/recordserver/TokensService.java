package com.swirlds.recordserver;

import com.swirlds.recordserver.util.QueryParamUtil;
import io.helidon.common.http.Http.Status;
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
import static com.swirlds.recordserver.util.Utils.addIfKeyNotNull;
import static com.swirlds.recordserver.util.Utils.addIfNotNull;
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
        rules.get("/", this::listTokens);
        rules.get("/{tokenId}", this::getTokenById);
        rules.get("/{tokenId}/balances", this::listTokenBalancesById);
    }

    private void listTokens(ServerRequest request, ServerResponse response) {
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
        System.out.println("listTokens(): queryString = " + queryString);
        final PreparedStatement statement = this.pinotConnection.prepareStatement(new Request("sql", queryString));
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
            JsonObjectBuilder singleObject = JSON.createObjectBuilder();
            addIfKeyNotNull(singleObject, "admin_key", fields, "adminKey");
            addIfNotNull(singleObject, "alias", resultTableResultSet.getString(i, 3));
            addIfNotNull(singleObject, "auto_renew_account", fields.getString("autoRenewAccount", null));
            addIfNotNull(singleObject, "auto_renew_period", fields.getString("autoRenewPeriod", null));
            singleObject.add("consensus_timestamp", consensusTime);
            addIfNotNull(singleObject, "decimals", fields.getString("decimals", null));
            singleObject.add("entity_number", resultTableResultSet.getLong(i, 1));
            addIfNotNull(singleObject, "evm_address", resultTableResultSet.getString(i, 2));
            addIfNotNull(singleObject, "expiry", fields.getString("expiry", null));
            addIfKeyNotNull(singleObject, "fee_schedule_key", fields, "feeScheduleKey");
            addIfNotNull(singleObject, "freeze_default", fields.getString("freezeDefault", null));
            addIfKeyNotNull(singleObject, "freeze_key", fields, "freezeKey");
            addIfNotNull(singleObject, "initial_supply", fields.getString("initialSupply", null));
            addIfKeyNotNull(singleObject, "kyc_key", fields, "kycKey");
            addIfNotNull(singleObject, "max_supply", fields.getString("maxSupply", null));
            addIfNotNull(singleObject, "memo", fields.getString("memo", null));
            addIfNotNull(singleObject, "name", fields.getString("name", null));
            addIfKeyNotNull(singleObject, "pause_key", fields, "pauseKey");
            addIfNotNull(singleObject, "public_key", resultTableResultSet.getString(i, 5));
            addIfNotNull(singleObject, "public_key_type", resultTableResultSet.getString(i, 4));
            addIfNotNull(singleObject, "realm", fields.getString("realm", null));
            addIfNotNull(singleObject, "shard", fields.getString("shard", null));
            addIfKeyNotNull(singleObject, "supply_key", fields, "supplyKey");
            addIfNotNull(singleObject, "supply_type", fields.getString("supplyType", null));
            addIfNotNull(singleObject, "symbol", fields.getString("symbol", null));
            addIfNotNull(singleObject, "token_type", fields.getString("tokenType", null));
            addIfNotNull(singleObject, "treasury", fields.getString("treasury", null));
            addIfKeyNotNull(singleObject, "wipe_key", fields, "wipeKey");
            tokensArray.add(singleObject.build());
        }
        final JsonObjectBuilder returnObject = JSON.createObjectBuilder()
                .add("tokens", tokensArray.build())
                .add("links", JSON.createObjectBuilder()
                        .add("next","/api/v1/tokens?order=" + direction + "&limit=" + limit + "&consensus_timestamp=" +
                                (direction.equalsIgnoreCase("asc") ? "gt" : "lt") + latestConsensusTime)
                        .build());
        response.send(returnObject.build());
    }

/**********************
### getTokenById not specifying a timestamp
GET http://localhost:8080/api/v1/tokens/107594
### getTokenById specifying a timestamp
GET http://localhost:8080/api/v1/tokens/107594?timestamp=1610640452612903002
### getTokenById specifying an earlier timestamp
GET http://localhost:8080/api/v1/tokens/107594?timestamp=1610640421985772001
### getTokenById specifying an earlier timestamp than the passed-in one
GET http://localhost:8080/api/v1/tokens/107594?timestamp=lt:1610640452612903002
**********************/
    private void getTokenById(ServerRequest request, ServerResponse response) {
        System.out.println("Calling into getTokenById!");
	
        final Optional<String> timestampQueryParam = request.queryParams().first("timestamp");

        // build and execute query
        final List<QueryParamUtil.WhereClause> whereClauses = new ArrayList<>();
        // always restrict query (over all entities) to only return tokens
        whereClauses.add(new QueryParamUtil.WhereClause(QueryParamUtil.Type._string, "type",
                QueryParamUtil.Comparator.eq, "TOKEN"));
        // restrict query to only the tokenId passed in as a path parameter
        whereClauses.add(QueryParamUtil.parseQueryString(QueryParamUtil.Type._long, "entity_number",
                request.path().segments().get(0)));
        timestampQueryParam.ifPresent(s -> whereClauses.add(
                QueryParamUtil.parseQueryString(QueryParamUtil.Type._long, "consensus_timestamp", s)));

        final String whereClause = whereClauses.isEmpty() ? "" : "where " +
                QueryParamUtil.whereClausesToQuery(whereClauses);
        final int limit = 1; // we only want to get one token entity per API call.
        final String queryString =
                "select consensus_timestamp, entity_number, evm_address, alias, public_key_type, public_key, fields " +
                "from entity " + whereClause + " order by consensus_timestamp desc limit ?";
        System.out.println("getTokenById(): queryString = " + queryString);
        final PreparedStatement statement = this.pinotConnection.prepareStatement(new Request("sql", queryString));
        QueryParamUtil.applyWhereClausesToQuery(whereClauses, statement);
        statement.setInt(whereClauses.size(), limit);
        final ResultSetGroup pinotResultSetGroup = statement.execute();
        final ResultSet resultTableResultSet = pinotResultSetGroup.getResultSet(0);

        // format results to JSON
        final JsonObjectBuilder returnObject = JSON.createObjectBuilder();
        if (resultTableResultSet.getRowCount() < 1) {
            response.status(Status.NOT_FOUND_404);
            response.send();
            return;
        }

        // this shouldn't happen (because we explicitly set LIMIT to 1), but at least indicate that something is up.
        if (resultTableResultSet.getRowCount() > 1) {
            returnObject.add("count", resultTableResultSet.getRowCount());
        }

        final long consensusTime = resultTableResultSet.getLong(0, 0);
        final JsonObject fields = parseFromColumn(resultTableResultSet.getString(0, 6));

        addIfKeyNotNull(returnObject, "admin_key", fields, "adminKey");
        returnObject.add("alias", resultTableResultSet.getString(0, 3));
        addIfNotNull(returnObject, "auto_renew_account", fields.getString("autoRenewAccount", null));
        addIfNotNull(returnObject, "auto_renew_period", fields.getString("autoRenewPeriod", null));
        returnObject.add("created_timestamp", consensusTime);
        // no data retained for "deleted" (boolean) field
        addIfNotNull(returnObject, "decimals", fields.getJsonNumber("decimals"));
        returnObject.add("evm_address", resultTableResultSet.getString(0, 2));
        addIfNotNull(returnObject, "expiry_timestamp", fields.getJsonNumber("expiry"));
        addIfNotNull(returnObject, "freeze_default", fields.getBoolean("freezeDefault", false));
        addIfKeyNotNull(returnObject, "freeze_key", fields, "freezeKey");
        addIfNotNull(returnObject, "initial_supply", fields.getString("initialSupply", null));
        addIfKeyNotNull(returnObject, "kyc_key", fields, "kycKey");
        addIfNotNull(returnObject, "max_supply", fields.getString("maxSupply", null));
        addIfNotNull(returnObject, "memo", fields.getString("memo", null));
        returnObject.add("modified_timestamp", consensusTime);
        addIfNotNull(returnObject, "name", fields.getString("name", null));
        addIfKeyNotNull(returnObject, "pause_key", fields, "pauseKey");
        // no data retained for pause_status field
        addIfNotNull(returnObject, "public_key", resultTableResultSet.getString(0, 5));
        addIfNotNull(returnObject, "public_key_type", resultTableResultSet.getString(0, 4));
        addIfNotNull(returnObject, "realm", fields.getString("realm", null));
        addIfNotNull(returnObject, "shard", fields.getString("shard", null));
        addIfKeyNotNull(returnObject, "supply_key", fields, "supplyKey");
        addIfNotNull(returnObject, "supply_type", fields.getString("supplyType", null));
        addIfNotNull(returnObject, "symbol", fields.getString("symbol", null));
        returnObject.add("token_id", resultTableResultSet.getLong(0, 1));
        // no data retained for total_supply field
        addIfNotNull(returnObject, "treasury_account_id", fields.getString("treasury", null));
        addIfNotNull(returnObject, "type", fields.getString("tokenType", null));
        addIfKeyNotNull(returnObject, "wipe_key", fields, "wipeKey");
        // no data retained for custom_fees field

        response.send(returnObject.build());
    }

/**********************
### listTokenBalancesById specifying only a token id
GET http://localhost:8080/api/v1/tokens/629591/balances?limit=99
### listTokenBalancesById specifying a token id and an account id
GET http://localhost:8080/api/v1/tokens/629591/balances?account.id=644972
### listTokenBalancesById specifying a token id and a range of account ids
GET http://localhost:8080/api/v1/tokens/629591/balances?account.id=gt:644000&account.id=lt:644999&limit=1000
### listTokenBalancesById specifying a token id and a range of account ids, sorted in descending order
GET http://localhost:8080/api/v1/tokens/629591/balances?account.id=gt:644000&account.id=lt:644999&limit=1000&order=desc
### listTokenBalancesById specifying a token id and an account id and a particular balance amount
GET http://localhost:8080/api/v1/tokens/629591/balances?account.id=644972&account.balance=500000000
### listTokenBalancesById specifying a token id and an account id and a particular timestamp -- not yet working
GET http://localhost:8080/api/v1/tokens/629591/balances?account.id=644972&timestamp=1644887406787336960
 **********************/
    private void listTokenBalancesById(ServerRequest request, ServerResponse response) {
        // this routine has much in common with BalanceService.getDefaultMessageHandler(); they should be kept in sync.

        final Optional<String> accountIdQueryParam = request.queryParams().first("account.id");
        final Optional<String> accountBalanceQueryParam = request.queryParams().first("account.balance");
        final Optional<String> orderParam = request.queryParams().first("order");
        final Optional<String> publicKeyQueryParam = request.queryParams().first("account.publickey");
        final Optional<String> timestampQueryParam = request.queryParams().first("timestamp"); // TODO (MYK): can be a list
        final Optional<String> limitParam = request.queryParams().first("limit");

        // TODO if publicKeyParam is set then need to query accounts table to get account ID from public key, then look

        // build and execute query
        final List<QueryParamUtil.WhereClause> whereClauses = new ArrayList<>();
        final int limit = parseLimitQueryString(limitParam);
        System.out.println("listTokenBalancesById: limit = " + limit);
        final QueryParamUtil.WhereClause accountWhereClause = (accountIdQueryParam.isPresent() ?
                QueryParamUtil.parseQueryString(QueryParamUtil.Type._long, "account_id", accountIdQueryParam.get()) :
                null);
        System.out.println("accountWhereClause = " + accountWhereClause);
        // restrict query to only the tokenId passed in as a path parameter
        final String tokenId = request.path().segments().get(0);
        accountBalanceQueryParam.ifPresent(s -> whereClauses.add(
                QueryParamUtil.parseQueryString(QueryParamUtil.Type._long, "balance", s)));
        timestampQueryParam.ifPresent(s -> whereClauses.add(
                QueryParamUtil.parseQueryString(QueryParamUtil.Type._long, "consensus_timestamp", s)));

        long minAccount, maxAccount;
        boolean singleAccountMode = false;
        if (accountIdQueryParam.isPresent()) {
            if (accountWhereClause.comparator() == QueryParamUtil.Comparator.eq) {
                whereClauses.add(accountWhereClause);
                singleAccountMode = true;
            } else if (accountWhereClause.comparator() == QueryParamUtil.Comparator.ne){
                // TODO, not quite sure what this should do
                singleAccountMode = false;
            } else {
                switch (accountWhereClause.comparator()) {
                    case lt -> {
                        maxAccount = Long.parseLong(accountWhereClause.value());
                        minAccount = maxAccount - limit;
                    }
                    case lte -> {
                        maxAccount = Long.parseLong(accountWhereClause.value()) + 1;
                        minAccount = maxAccount - limit;
                    }
                    case gt -> {
                        minAccount = Long.parseLong(accountWhereClause.value());
                        maxAccount = minAccount + limit;
                    }
                    case gte -> {
                        minAccount = Long.parseLong(accountWhereClause.value()) - 1;
                        maxAccount = minAccount + limit;
                    }
                    default -> {
                        minAccount = 0;
                        maxAccount = limit;
                    }
                }
                whereClauses.add(new QueryParamUtil.WhereClause(QueryParamUtil.Type._long, "account_id",
                        QueryParamUtil.Comparator.gt, Long.toString(minAccount)));
                whereClauses.add(new QueryParamUtil.WhereClause(QueryParamUtil.Type._long, "account_id",
                        QueryParamUtil.Comparator.lt, Long.toString(maxAccount)));
            }
        } else {
            minAccount = 0;
            maxAccount = Long.MAX_VALUE;
            whereClauses.add(new QueryParamUtil.WhereClause(QueryParamUtil.Type._long, "account_id",
                    QueryParamUtil.Comparator.gt, Long.toString(minAccount)));
            whereClauses.add(new QueryParamUtil.WhereClause(QueryParamUtil.Type._long, "account_id",
                    QueryParamUtil.Comparator.lt, Long.toString(maxAccount)));
        }

        final String direction = QueryParamUtil.Order.parse(orderParam).toString();
        whereClauses.add(QueryParamUtil.parseQueryString(QueryParamUtil.Type._long, "token_id", tokenId));
        final String whereClause = "where " + QueryParamUtil.whereClausesToQuery(whereClauses);
        System.out.println("        whereClause = " + whereClause);
        final String queryString =
                "select account_id, token_id, LASTWITHTIME(balance, consensus_timestamp, 'long') as balance, " +
                        "max(consensus_timestamp) as consensus_timestamp from balance " + whereClause + 
                        " group by account_id, token_id order by account_id " + direction + " limit ?";
        System.out.println("queryString = " + queryString);
        final PreparedStatement statement = this.pinotConnection.prepareStatement(new Request("sql", queryString));
        QueryParamUtil.applyWhereClausesToQuery(whereClauses, statement);
        statement.setInt(whereClauses.size(), limit);
        final ResultSetGroup pinotResultSetGroup = statement.execute();
        final ResultSet resultTableResultSet = pinotResultSetGroup.getResultSet(0);

        // format results to JSON
        long highestAccountNumber = (direction.equalsIgnoreCase("asc") ? 0 : Long.MAX_VALUE);
        final JsonArrayBuilder balancesArray = JSON.createArrayBuilder();
        for (int i = 0; i < resultTableResultSet.getRowCount(); i++) {
            final long accountNum = resultTableResultSet.getLong(i, 0);
            highestAccountNumber = (direction.equalsIgnoreCase("asc") ? Math.max(highestAccountNumber, accountNum) :
                    Math.min(highestAccountNumber, accountNum));
            balancesArray.add(JSON.createObjectBuilder()
                    .add("account", "0.0." + accountNum)
                    .add("balance", resultTableResultSet.getLong(i, 2))
                    .add("tokens",JSON.createArrayBuilder().build())
                    .build());
        }

        final JsonObjectBuilder returnObject = JSON.createObjectBuilder()
                .add("timestamp", (resultTableResultSet.getRowCount() <= 0 ? 0
                        : (long)resultTableResultSet.getDouble(0, 3)))
                .add("balances", balancesArray.build());
        if (!singleAccountMode) {
            String nextLink = "/api/v1/tokens/" + tokenId + "/balances?order=" + direction + "&limit=" + limit +
                    "&account.id=" + (direction.equalsIgnoreCase("asc") ? "gt" : "lt") + ":0.0." + highestAccountNumber;
            returnObject.add("links", JSON.createObjectBuilder()
                        .add("next", nextLink)
                        .build());
        }
        response.send(returnObject.build());
    }

}
