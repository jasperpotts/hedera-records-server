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

    public enum transactionType {CREDIT,DEBIT };
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
        rules.get("/", this::getTransactionIdMessageHandler);
    }

    private void getTransactionIdMessageHandler(ServerRequest request, ServerResponse response) {
        System.out.println("The request path is " + request.queryParams());
        if(!request.queryParams().toMap().containsKey("transactionId")) {
             getTransactionsMessageHandler(request, response);
             return;
        }
        final Optional<String> nonceParam = request.queryParams().first("nonce");
        final Optional<String> scheduledParam = request.queryParams().first("scheduled");
        final Optional<String> transactionIdParam = request.queryParams().first("transactionId");

        // build and execute query
        final List<QueryParamUtil.WhereClause> whereClauses = new ArrayList<>();

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
            final JsonObjectBuilder returnObject = getTransactionsJsonObjectBuilder(resultTableResultSet, 0, fields, transfers);
            response.send(returnObject.build());
        } else {
            response.send(Json.createObjectBuilder().build());
        }
    }

    private void getTransactionsMessageHandler(ServerRequest request, ServerResponse response) {

        final Optional<String> accountIdParam = request.queryParams().first("account.id");
        final Optional<String> limitParam = request.queryParams().first("limit");
        final Optional<String> orderParam = request.queryParams().first("order");
        final Optional<String> timestampsParam = request.queryParams().first("timestamp");
        final Optional<String> transactionTypeParam = request.queryParams().first("transactiontype");
        final Optional<String> resultParam = request.queryParams().first("result");
        final Optional<String> typeParam = request.queryParams().first("type");

        // build and execute query
        final List<QueryParamUtil.WhereClause> whereClauses = new ArrayList<>();
        final int limit = parseLimitQueryString(limitParam);
        if(accountIdParam.isPresent()) {
            final QueryParamUtil.WhereClause accountWhereClause =
                    QueryParamUtil.parseQueryString(QueryParamUtil.Type._long, "ids", accountIdParam.get()) ;
            whereClauses.add(accountWhereClause);
            if(typeParam.isPresent()) {
                final QueryParamUtil.WhereClause typeWhereClause = checkCreditDebitType(typeParam, accountIdParam);
                System.out.println("  type where = " + typeWhereClause);
                if(typeWhereClause!=null)
                    whereClauses.add(typeWhereClause);
            }
        }

        timestampsParam.ifPresent(s -> whereClauses.add(QueryParamUtil.parseQueryString(QueryParamUtil.Type._string,"timestamp",s)));
        transactionTypeParam.ifPresent(s -> whereClauses.add(QueryParamUtil.parseQueryString(QueryParamUtil.Type._string,"type",s)));
        resultParam.ifPresent(s -> whereClauses.add(QueryParamUtil.parseQueryString(QueryParamUtil.Type._string,"result",s)));

        final String whereClause = whereClauses.isEmpty() ? "" : "where "+QueryParamUtil.whereClausesToQuery(whereClauses);
        final String queryString =
                "select * from transaction " +
                        whereClause + " order by consensus_timestamp "+QueryParamUtil.Order.parse(orderParam).toString()+" limit ?";
        System.out.println("\nqueryString = " + queryString);
        final PreparedStatement statement = this.pinotConnection.prepareStatement(new Request("sql",queryString));
        QueryParamUtil.applyWhereClausesToQuery(whereClauses, statement);
        statement.setInt(whereClauses.size(), limit);
            final ResultSetGroup pinotResultSetGroup = statement.execute();
        final ResultSet resultTableResultSet = pinotResultSetGroup.getResultSet(0);
            final JsonArrayBuilder transactionsArray = JSON.createArrayBuilder();
            System.out.println("ResultSet count " + resultTableResultSet.getRowCount());
            for (int i = 0; i < resultTableResultSet.getRowCount(); i++) {
                final JsonObject fields = parseFromColumn(resultTableResultSet.getString(i, 8));
                final JsonArray transfers = parseArrayFromColumn(resultTableResultSet.getString(i, 15));
                transactionsArray.add(getTransactionsJsonObjectBuilder(resultTableResultSet, i, fields, transfers).build());
            }
                final JsonObjectBuilder returnObject = JSON.createObjectBuilder()
                .add("transactions", transactionsArray.build())
                .add("links", JSON.createObjectBuilder().add("next","/api/v1/transactions?order=" + orderParam.get() + "&limit=" + limit).build());
        response.send(returnObject.build());
    }

    public static JsonObjectBuilder getTransactionsJsonObjectBuilder(ResultSet resultTableResultSet, int i, JsonObject fields, JsonArray transfers) {
        return JSON.createObjectBuilder()
                .add("assessed_custom_fees", resultTableResultSet.getString(i, 0))
                .add("consensus_timestamp", resultTableResultSet.getLong(i, 1))
                .add("contract_logs", resultTableResultSet.getString(i, 2))
                .add("contract_results", resultTableResultSet.getString(i, 3))
                .add("contract_state_change", resultTableResultSet.getString(i, 4))
                .add("entityId", resultTableResultSet.getLong(i, 7))
                .add("bytes", fields.getString("transaction_bytes"))
                .add("charged_tx_fee", fields.getInt("charged_tx_fee"))
                .add("max_fee", fields.getInt("max_fee"))
                .add("memo", fields.getString("memo"))
                .add("valid_duration_seconds", fields.getInt("valid_duration_seconds"))
                .add("valid_start_ns", fields.get("valid_start_ns"))
                .add("parent_consensus_timestamp", fields.get("parent_consensus_timestamp"))
                .add("transaction_hash", fields.getString("transaction_hash"))
                .add("result", resultTableResultSet.getString(i, 12))
                .add("scheduled", resultTableResultSet.getString(i, 13))
                .add("transaction_id", resultTableResultSet.getString(i, 14))
                .add("transfers", transfers);
    }

    private QueryParamUtil.WhereClause checkCreditDebitType(Optional<String> typeParam, Optional<String> accountIdParam) {
        System.out.println("Type PRAM " + typeParam.get() + "account Id" + accountIdParam.get());
                if(typeParam.get().equals(transactionType.CREDIT.name())) {
                    return QueryParamUtil.parseQueryString(QueryParamUtil.Type._long, "credited_ids", accountIdParam.get());
                } else if(typeParam.get().equals(transactionType.DEBIT.name())) {
                    return QueryParamUtil.parseQueryString(QueryParamUtil.Type._long, "debited_ids", accountIdParam.get());
                }
        return null;
    }


}
