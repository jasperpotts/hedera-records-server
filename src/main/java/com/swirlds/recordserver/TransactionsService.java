package com.swirlds.recordserver;

import com.swirlds.recordserver.util.QueryParamUtil;
import io.helidon.config.Config;
import io.helidon.metrics.api.RegistryFactory;
import io.helidon.webserver.Routing;
import io.helidon.webserver.ServerRequest;
import io.helidon.webserver.ServerResponse;
import io.helidon.webserver.Service;
import jakarta.json.*;
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
import static com.swirlds.recordserver.util.Utils.parseArrayFromColumn;

/**
 * A service for transactions API
 */

public class TransactionsService implements Service {

    private static final Logger LOGGER = Logger.getLogger(TransactionsService.class.getName());
    private static final JsonBuilderFactory JSON = Json.createBuilderFactory(Collections.emptyMap());
    private final MetricRegistry registry = RegistryFactory.getInstance().getRegistry(MetricRegistry.Type.APPLICATION);
    private final Counter accessCtr = registry.counter("accessctr");
    private final Connection pinotConnection;

    public TransactionsService(Config config) {
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
        final Optional<String> nonceParam = request.queryParams().first("nonce");
        final Optional<String> scheduledParam = request.queryParams().first("scheduled");
        final Optional<String> transactionIdParam = request.queryParams().first("transactionId");

        // build and execute query
        final List<QueryParamUtil.WhereClause> whereClauses = new ArrayList<>();

        for (var where: whereClauses) {
            System.out.println("        where = " + where);
        }

        nonceParam.ifPresent(s -> whereClauses.add(QueryParamUtil.parseQueryString(QueryParamUtil.Type._string,"nonce",s)));
        scheduledParam.ifPresent(s -> whereClauses.add(QueryParamUtil.parseQueryString(QueryParamUtil.Type._string,"scheduled",s)));
        transactionIdParam.ifPresent(s -> whereClauses.add(QueryParamUtil.parseQueryString(QueryParamUtil.Type._string,"transaction_id",s)));

        final String whereClause = whereClauses.isEmpty() ? "" : "where "+QueryParamUtil.whereClausesToQuery(whereClauses);
        final String queryString =
                "select * from transaction " +
                        whereClause;
        System.out.println("\nqueryString = " + queryString);

        final PreparedStatement statement = this.pinotConnection.prepareStatement(new Request("sql",queryString));
        QueryParamUtil.applyWhereClausesToQuery(whereClauses, statement);
        final ResultSetGroup pinotResultSetGroup = statement.execute();
        final ResultSet resultTableResultSet = pinotResultSetGroup.getResultSet(0);
        if(resultTableResultSet.getRowCount()>0)
        {
            final JsonObject fields = parseFromColumn(resultTableResultSet.getString(0, 8));
            final JsonArray transfers = parseArrayFromColumn(resultTableResultSet.getString(0, 15));
            final JsonObjectBuilder returnObject = JSON.createObjectBuilder()
                    .add("assessed_custom_fees", resultTableResultSet.getString(0, 0))
                    .add("consensus_timestamp", resultTableResultSet.getLong(0, 1))
                    .add("contract_logs", resultTableResultSet.getString(0, 2))
                    .add("contract_results", resultTableResultSet.getString(0, 3))
                    .add("contract_state_change", resultTableResultSet.getString(0, 4))
                    .add("entityId", resultTableResultSet.getLong(0, 7))
                    .add("bytes", fields.getString("transaction_bytes"))
                    .add("charged_tx_fee", fields.getInt("charged_tx_fee"))
                    .add("max_fee", fields.getInt("max_fee"))
                    .add("memo", fields.getString("memo"))
                    .add("valid_duration_seconds", fields.getInt("valid_duration_seconds"))
                    .add("valid_start_ns", fields.get("valid_start_ns"))
                    .add("parent_consensus_timestamp", fields.get("parent_consensus_timestamp"))
                    .add("transaction_hash", fields.getString("transaction_hash"))
                    .add("result", resultTableResultSet.getString(0, 12))
                    .add("scheduled", resultTableResultSet.getString(0, 13))
                    .add("transaction_id", resultTableResultSet.getString(0, 14))
                    .add("transfers", transfers);
            response.send(returnObject.build());
        } else {
            response.send(Json.createObjectBuilder().build());
        }
    }


}
