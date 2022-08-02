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

        accountIdParam.ifPresent(s -> whereClauses.add(QueryParamUtil.parseQueryString(QueryParamUtil.Type._long,"account_id",s)));

        accountBalanceQueryParam.ifPresent(s -> whereClauses.add(QueryParamUtil.parseQueryString(QueryParamUtil.Type._long,"balance",s)));
        timestampsParam.ifPresent(s -> whereClauses.add(QueryParamUtil.parseQueryString(QueryParamUtil.Type._long,"consensus_timestamp",s)));
        final String whereClause = whereClauses.isEmpty() ? "" : "where "+QueryParamUtil.whereClausesToQuery(whereClauses);
        final String queryString =
                "select account_id, token_id, LASTWITHTIME(balance,consensus_timestamp,'long') as balance, max(consensus_timestamp) as consensus_timestamp from balance " +
                        whereClause+" group by account_id, token_id order by account_id "+QueryParamUtil.Order.parse(orderParam).toString()+" limit ?";
        System.out.println("queryString = " + queryString);
        final PreparedStatement statement = this.pinotConnection.prepareStatement(new Request("sql",queryString));
        QueryParamUtil.applyWhereClausesToQuery(whereClauses, statement);
        statement.setInt(whereClauses.size(), parseLimitQueryString(limitParam)); // limit
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
                .add("timestamp", (long)resultTableResultSet.getDouble(0,3))
                .add("balances", balancesArray.build())
                .add("links", JSON.createObjectBuilder()
                        .add("next","/api/v1/balances?order=asc&limit=10&account.id=gt:0.0."+highestAccountNumber)
                        .build())
                ;
        response.send(returnObject.build());
    }
}
