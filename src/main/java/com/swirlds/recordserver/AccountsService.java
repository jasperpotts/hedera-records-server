package com.swirlds.recordserver;

import com.swirlds.recordserver.util.QueryParamUtil;
import io.helidon.config.Config;
import io.helidon.metrics.api.RegistryFactory;
import io.helidon.webserver.Routing;
import io.helidon.webserver.ServerRequest;
import io.helidon.webserver.ServerResponse;
import io.helidon.webserver.Service;
import jakarta.json.*;
import org.apache.pinot.client.*;
import org.eclipse.microprofile.metrics.Counter;
import org.eclipse.microprofile.metrics.MetricRegistry;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.logging.Logger;

import static com.swirlds.recordserver.util.Utils.parseArrayFromColumn;
import static com.swirlds.recordserver.util.Utils.parseFromColumn;

public class AccountsService implements Service {

    private static final Logger LOGGER = Logger.getLogger(AccountsService.class.getName());
    private static final JsonBuilderFactory JSON = Json.createBuilderFactory(Collections.emptyMap());
    private final MetricRegistry registry = RegistryFactory.getInstance().getRegistry(MetricRegistry.Type.APPLICATION);
    private final Counter accessCtr = registry.counter("accessctr");
    private final Connection pinotConnection;

    public AccountsService(Config config) {
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

        final Optional<String> idParam = request.queryParams().first("idOrAliasOrEvmAddress");
        final Optional<String> transactionTypeParam = request.queryParams().first("type");

        CompletableFuture<JsonObjectBuilder> future1
                = CompletableFuture.supplyAsync(() -> getAccountsJsonFields(idParam));
        CompletableFuture<JsonObjectBuilder> future2
                = CompletableFuture.supplyAsync(() -> getTransactionsJsonFields(transactionTypeParam, idParam));
        CompletableFuture<JsonObjectBuilder> future3
                = CompletableFuture.supplyAsync(() -> getBalanceJsonFields(idParam));

        // Account/Entity where clauses
       /* final JsonObjectBuilder accountsReturnObject = getAccountsJsonFields(idParam);
        final JsonObjectBuilder transactionsReturnObject = getTransactionsJsonFields(transactionTypeParam);
        final JsonObjectBuilder balanceReturnObject = getBalanceJsonFields(idParam);
      */
        final JsonObjectBuilder returnObject;
        try {
            returnObject = JSON.createObjectBuilder()
                      .addAll(future1.get())
                      .addAll(future2.get())
                      .add("balance",future3.get());
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
        response.send(returnObject.build());
    }

    private JsonObjectBuilder getBalanceJsonFields(Optional<String> idParam) {
        final List<QueryParamUtil.WhereClause> whereClauses = new ArrayList<>();
        idParam.ifPresent(s -> whereClauses.add(QueryParamUtil.parseQueryString(QueryParamUtil.Type._long,"account_id",s)));
        final String whereClause = whereClauses.isEmpty() ? "" : "where "+QueryParamUtil.whereClausesToQuery(whereClauses);
        final String queryString =
                "select LASTWITHTIME(balance,consensus_timestamp,'long') as balance, max(consensus_timestamp) as consensus_timestamp from balance " +
                        whereClause+" group by account_id order by account_id asc";
        final PreparedStatement statement = this.pinotConnection.prepareStatement(new Request("sql",queryString));
        QueryParamUtil.applyWhereClausesToQuery(whereClauses, statement);
        final ResultSetGroup pinotResultSetGroup = statement.execute();
        final ResultSet resultTableResultSet = pinotResultSetGroup.getResultSet(0);
        final JsonObjectBuilder returnObject =  JSON.createObjectBuilder()
                                                .add("timestamp",resultTableResultSet.getDouble(0,1))
                                                .add("balance",resultTableResultSet.getLong(0,0))
                                                .add("tokens",JSON.createArrayBuilder().build());
        return returnObject;
    }

    private JsonObjectBuilder getTransactionsJsonFields(Optional<String> transactionTypeParam, Optional<String> idParam) {
        final List<QueryParamUtil.WhereClause> whereClauses = new ArrayList<>();
        transactionTypeParam.ifPresent(s -> whereClauses.add(QueryParamUtil.parseQueryString(QueryParamUtil.Type._string,"type",s)));
        final ResultSet resultTableResultSet = getResultSet(idParam, whereClauses, "ids","transaction");
        final JsonArrayBuilder transactionsArray = JSON.createArrayBuilder();
        for (int i = 0; i < resultTableResultSet.getRowCount(); i++) {
            final JsonObject fields = parseFromColumn(resultTableResultSet.getString(i, 8));
            final JsonArray transfers = parseArrayFromColumn(resultTableResultSet.getString(i, 15));
            transactionsArray.add(TransactionsService.getTransactionsJsonObjectBuilder(resultTableResultSet, i, fields, transfers).build());
        }
        final JsonObjectBuilder returnObject = JSON.createObjectBuilder()
                .add("transactions", transactionsArray.build());
        return returnObject;
    }

    private JsonObjectBuilder getAccountsJsonFields(Optional<String> idParam) {
        final List<QueryParamUtil.WhereClause> whereClauses = new ArrayList<>();
        final ResultSet resultTableSet = getResultSet(idParam, whereClauses, "entity_number", "entity");
        int i=0;// rowIndex
        final JsonObject fields = parseFromColumn(resultTableSet.getString(i, 4));
        final JsonObjectBuilder returnObject =  JSON.createObjectBuilder()
                 .add("account", resultTableSet.getLong(i, 2) )
                .add("alias", resultTableSet.getLong(i, 1) )
                .add("auto_renew_period", fields.getString("auto_renew_period",""))
                .add("decline_reward", fields.getString("decline_reward",""))
                .add("deleted", fields.getString("deleted",""))
                .add("ethereum_nonce", fields.getString("ethereum_nonce",""))
                .add("expiry_timestamp", fields.getString("expiry_timestamp",""))
                .add("key", JSON.createObjectBuilder()
                                .add("_type", resultTableSet.getString(i, 6))
                                .add("key", resultTableSet.getString(i, 5)))
                .add("max_automatic_token_associations", fields.getString("max_automatic_token_associations",""))
                .add("memo", fields.getString("memo",""))
                .add("receiver_sig_required", fields.getString("receiver_sig_required",""))
                .add("staked_account_id", fields.getString("staked_account_id",""))
                .add("staked_node_id", fields.getString("staked_node_id",""))
                .add("stake_period_start", fields.getString("stake_period_start",""))
                .add("evm_address", resultTableSet.getString(i, 3));
        return returnObject;
    }

    private ResultSet getResultSet(Optional<String> param, List<QueryParamUtil.WhereClause> whereClauses, String column_name, String tableName) {
        param.ifPresent(s -> whereClauses.add(QueryParamUtil.parseQueryString(QueryParamUtil.Type._long,column_name,s)));
        final String whereClause = whereClauses.isEmpty() ? "" : "where "+QueryParamUtil.whereClausesToQuery(whereClauses);
        final String queryString =
                "select * from " + tableName + " "+
                        whereClause;
        final PreparedStatement statement = this.pinotConnection.prepareStatement(new Request("sql",queryString));
        QueryParamUtil.applyWhereClausesToQuery(whereClauses, statement);
        final ResultSetGroup pinotResultSetGroup = statement.execute();
        final ResultSet resultTableResultSet = pinotResultSetGroup.getResultSet(0);
        return resultTableResultSet;
    }
}
