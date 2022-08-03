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

/**
 * A service for balances API
 */
public class BalancesService implements Service {

    private static final Logger LOGGER = Logger.getLogger(BalancesService.class.getName());
    private static final JsonBuilderFactory JSON = Json.createBuilderFactory(Collections.emptyMap());
    private final MetricRegistry registry = RegistryFactory.getInstance().getRegistry(MetricRegistry.Type.APPLICATION);
    private final Counter accessCtr = registry.counter("accessctr");
    private final Connection pinotConnection;

    BalancesService(Config config) {
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
        final Optional<String> accountIdParam = request.queryParams().first("account.id");
        final Optional<String> accountBalanceQueryParam = request.queryParams().first("account.balance");
        final Optional<String> accountPubKeyParam = request.queryParams().first("account.publickey");
        final Optional<String> timestampsParam = request.queryParams().first("timestamp");
        final Optional<String> limitParam = request.queryParams().first("limit");
        final Optional<String> orderParam = request.queryParams().first("order");

        // TODO if accountPubKeyParam is set then need to query accounts table to get account ID from public key, then look

        // build and execute query
        final List<QueryParamUtil.WhereClause> whereClauses = new ArrayList<>();
        final int limit = parseLimitQueryString(limitParam);
        System.out.println("limit = " + limit);
        final QueryParamUtil.WhereClause accountWhereClause = accountIdParam.isPresent() ?
                QueryParamUtil.parseQueryString(QueryParamUtil.Type._long,"account_id",accountIdParam.get()) : null;
        System.out.println("accountWhereClause = " + accountWhereClause);

        long minAccount, maxAccount;
        boolean singleAccountMode = false;
        if (accountIdParam.isPresent()) {
            if (accountWhereClause.comparator() == QueryParamUtil.Comparator.eq) {
                whereClauses.add(accountWhereClause);
                singleAccountMode = true;
            } else if (accountWhereClause.comparator() == QueryParamUtil.Comparator.ne){
                // TODO, not quite sure what this should do
                singleAccountMode = true;
            } else {
                switch (accountWhereClause.comparator()) {
                    case lt -> {
                        maxAccount = Long.parseLong(accountWhereClause.value());
                        minAccount = maxAccount-limit;
                    }
                    case lte -> {
                        maxAccount = Long.parseLong(accountWhereClause.value())+1;
                        minAccount = maxAccount-limit;
                    }
                    case gt -> {
                        minAccount = Long.parseLong(accountWhereClause.value());
                        maxAccount = minAccount+limit;
                    }
                    case gte -> {
                        minAccount = Long.parseLong(accountWhereClause.value())-1;
                        maxAccount = minAccount+limit;
                    }
                    default -> {
                        minAccount = 0;
                        maxAccount = minAccount+limit;
                    }
                }
                whereClauses.add(new QueryParamUtil.WhereClause(QueryParamUtil.Type._long,"account_id",
                        QueryParamUtil.Comparator.gt,Long.toString(minAccount)));
                whereClauses.add(new QueryParamUtil.WhereClause(QueryParamUtil.Type._long,"account_id",
                        QueryParamUtil.Comparator.lt,Long.toString(maxAccount)));
            }
        } else {
            minAccount = 0;
            maxAccount = limit;
            whereClauses.add(new QueryParamUtil.WhereClause(QueryParamUtil.Type._long,"account_id",
                    QueryParamUtil.Comparator.gt,Long.toString(minAccount)));
            whereClauses.add(new QueryParamUtil.WhereClause(QueryParamUtil.Type._long,"account_id",
                    QueryParamUtil.Comparator.lt,Long.toString(maxAccount)));
        }
        for (var where: whereClauses) {
            System.out.println("        where = " + where);
        }

        accountBalanceQueryParam.ifPresent(s -> whereClauses.add(QueryParamUtil.parseQueryString(QueryParamUtil.Type._long,"balance",s)));
        timestampsParam.ifPresent(s -> whereClauses.add(QueryParamUtil.parseQueryString(QueryParamUtil.Type._long,"consensus_timestamp",s)));
        final String whereClause = whereClauses.isEmpty() ? "" : "where "+QueryParamUtil.whereClausesToQuery(whereClauses);
        final String queryString =
                "select account_id, token_id, LASTWITHTIME(balance,consensus_timestamp,'long') as balance, max(consensus_timestamp) as consensus_timestamp from balance " +
                        whereClause+" group by account_id, token_id order by account_id "+QueryParamUtil.Order.parse(orderParam).toString();
        System.out.println("queryString = " + queryString);
        final PreparedStatement statement = this.pinotConnection.prepareStatement(new Request("sql",queryString));
        QueryParamUtil.applyWhereClausesToQuery(whereClauses, statement);
        final ResultSetGroup pinotResultSetGroup = statement.execute();
        final ResultSet resultTableResultSet = pinotResultSetGroup.getResultSet(0);

        // format results to JSON
        long highestAccountNumber = 0;
        final JsonArrayBuilder balancesArray = JSON.createArrayBuilder();
        for (int i = 0; i < resultTableResultSet.getRowCount(); i++) {
            final long accountNum = resultTableResultSet.getLong(i,0);
            highestAccountNumber = Math.max(highestAccountNumber,accountNum);
            balancesArray.add(JSON.createObjectBuilder()
                    .add("account","0.0."+accountNum)
                    .add("balance",resultTableResultSet.getLong(i,2))
                    .add("tokens",JSON.createArrayBuilder().build())
                    .build());
        }
        final JsonObjectBuilder returnObject = JSON.createObjectBuilder()
                .add("timestamp", resultTableResultSet.getRowCount() <= 0 ? 0 : (long)resultTableResultSet.getDouble(0,3))
                .add("balances", balancesArray.build());
        if (!singleAccountMode) {
            returnObject.add("links", JSON.createObjectBuilder()
                        .add("next",
                                "/api/v1/balances?order=asc&limit=" + limit + "&account.id=gt:0.0." + highestAccountNumber)
                        .build())
                ;
        }
        response.send(returnObject.build());
    }
}
