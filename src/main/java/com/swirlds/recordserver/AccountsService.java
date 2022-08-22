package com.swirlds.recordserver;

import com.swirlds.recordserver.util.QueryParamUtil;
import io.helidon.config.Config;
import io.helidon.metrics.api.RegistryFactory;
import io.helidon.webserver.Routing;
import io.helidon.webserver.ServerRequest;
import io.helidon.webserver.ServerResponse;
import io.helidon.webserver.Service;
import jakarta.json.JsonObject;
import jakarta.json.JsonObjectBuilder;
import jakarta.json.JsonBuilderFactory;
import jakarta.json.Json;
import jakarta.json.JsonArrayBuilder;
import jakarta.json.JsonArray;
import org.apache.pinot.client.*;
import org.eclipse.microprofile.metrics.MetricRegistry;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.logging.Logger;

import static com.swirlds.recordserver.util.QueryParamUtil.parseLimitQueryString;
import static com.swirlds.recordserver.util.Utils.parseArrayFromColumn;
import static com.swirlds.recordserver.util.Utils.parseFromColumn;

public class AccountsService implements Service {

    private static final Logger LOGGER = Logger.getLogger(AccountsService.class.getName());
    private static final JsonBuilderFactory JSON = Json.createBuilderFactory(Collections.emptyMap());
    private final MetricRegistry registry = RegistryFactory.getInstance().getRegistry(MetricRegistry.Type.APPLICATION);

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
        rules.get("/{idOrAliasOrEvmAddress}/nfts", this::getAccountNftsMessageHandler);
        rules.get("/{idOrAliasOrEvmAddress}", this::getAccountsMessageHandler);
    }

    private void getAccountNftsMessageHandler(ServerRequest request, ServerResponse response) {
        final String idParam = request.path().param("idOrAliasOrEvmAddress");
        final Optional<String> serialNumberParam = request.queryParams().first("serialnumber");
        final Optional<String> tokenIdParam = request.queryParams().first("token.id");
        final Optional<String> spenderIdParam = request.queryParams().first("spender.id");
        final Optional<String> limitParam = request.queryParams().first("limit");
        final Optional<String> orderParam = request.queryParams().first("order");

        if(idParam.isBlank()) {
            response.send(getErrorMessage());
            return;
        }

        JsonObjectBuilder accountBuilder =  getAccountsJsonFields(idParam);
        if(accountBuilder == null) {
            response.send(JSON.createObjectBuilder().build());
            return;
        }

        final String id = getAccountId(accountBuilder.build());

        final List<QueryParamUtil.WhereClause> whereClauses = new ArrayList<>();
        final int limit = parseLimitQueryString(limitParam);
        whereClauses.add(QueryParamUtil.parseQueryString(QueryParamUtil.Type._string,"account_id",id));
        serialNumberParam.ifPresent(s -> whereClauses.add(QueryParamUtil.parseQueryString(QueryParamUtil.Type._string,"serial_number",s)));
        spenderIdParam.ifPresent(s -> whereClauses.add(QueryParamUtil.parseQueryString(QueryParamUtil.Type._string,"spender",s)));
        tokenIdParam.ifPresent(s -> whereClauses.add(QueryParamUtil.parseQueryString(QueryParamUtil.Type._string,"token_id",s)));
        String orderBy = getOrderBy(orderParam);

        final String whereClause = whereClauses.isEmpty() ? "" : "where "+QueryParamUtil.whereClausesToQuery(whereClauses);
        final String queryString =
                "select * from nft " +
                        whereClause + " order by consensus_timestamp "+orderBy+" limit ?";
        System.out.println("\nqueryString = " + queryString);
        final PreparedStatement statement = this.pinotConnection.prepareStatement(new Request("sql",queryString));
        QueryParamUtil.applyWhereClausesToQuery(whereClauses, statement);
        statement.setInt(whereClauses.size(), limit);
        final ResultSetGroup pinotResultSetGroup = statement.execute();
        final ResultSet resultTableSet = pinotResultSetGroup.getResultSet(0);
        final JsonArrayBuilder nftArray = JSON.createArrayBuilder();

        if(resultTableSet.getRowCount() == 0) {
            response.send(JSON.createObjectBuilder().build());
        }
        for (int i = 0; i < resultTableSet.getRowCount(); i++) {
                nftArray.add(JSON.createObjectBuilder()
                        .add("account_id", resultTableSet.getString(i, 0))
                        .add("created_timestamp", resultTableSet.getLong(i, 1))
                        .add("delegating_spender", resultTableSet.getString(i, 2))
                        .add("deleted", resultTableSet.getString(i, 3))
                        .add("metadata", resultTableSet.getString(i, 4))
                        .add("modified_timestamp", resultTableSet.getString(i, 1))
                        .add("serial_number", resultTableSet.getLong(i, 5))
                        .add("spender", resultTableSet.getString(i, 6))
                        .add("token_id", resultTableSet.getString(i, 7)).build());
        }

        final JsonObjectBuilder returnObject = JSON.createObjectBuilder()
                .add("nfts", nftArray.build())
                .add("links", JSON.createObjectBuilder().add("next","/api/v1/accounts/"+idParam+"/nfts?order=" + orderBy + "&limit=" + limit).build());
        response.send(returnObject.build());

    }

    private String getAccountId(JsonObject account) {
        String accountId = String.valueOf(account.get("account"));
        if(!accountId.isBlank()) {
            var ids = accountId.split(".");
            accountId = ids.length>1 ? ids[2] : accountId;
        }
        return accountId;
    }

    private String getOrderBy(Optional<String> orderParam) {
        String orderBy;
        if(orderParam.isPresent()){
            orderBy = QueryParamUtil.Order.parse(orderParam).toString();
        } else {
            orderBy = QueryParamUtil.ORDER_BY_DESC;
        }
        return orderBy;
    }

    private JsonObjectBuilder getErrorMessage() {
        return JSON.createObjectBuilder().add("error", "Id field is empty");
    }

    private void getAccountsMessageHandler(ServerRequest request, ServerResponse response) {

        final String idParam = request.path().param("idOrAliasOrEvmAddress");
        final Optional<String> transactionTypeParam = request.queryParams().first("type");
        final JsonObjectBuilder accountBuilder =  getAccountsJsonFields(idParam);
        if(accountBuilder == null) {
            response.send(JSON.createObjectBuilder().build());
            return;
        }
        //Reading the account number from the entity table.
        JsonObject account = accountBuilder.build();
        final String id = getAccountId(account);

        CompletableFuture<JsonObjectBuilder> future2
                = CompletableFuture.supplyAsync(() -> getTransactionsJsonFields(transactionTypeParam, id));
        CompletableFuture<JsonObjectBuilder> future3
                = CompletableFuture.supplyAsync(() -> getBalanceJsonFields(id));

        final JsonObjectBuilder returnObject;
        try {
            returnObject = JSON.createObjectBuilder(account)
                      .addAll(future2.get())
                      .add("balance",future3.get());
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
        response.send(returnObject.build());
    }

    private JsonObjectBuilder getBalanceJsonFields(String idParam) {
        final List<QueryParamUtil.WhereClause> whereClauses = new ArrayList<>();
        if(!idParam.isBlank()){
            whereClauses.add(QueryParamUtil.parseQueryString(QueryParamUtil.Type._long,"account_id",idParam));
        } else {
            return JSON.createObjectBuilder().add("error","Id field is empty - balance");
        }
        final String whereClause = whereClauses.isEmpty() ? "" : "where "+QueryParamUtil.whereClausesToQuery(whereClauses);
        final String queryString =
                "select LASTWITHTIME(balance,consensus_timestamp,'long') as balance, max(consensus_timestamp) as consensus_timestamp from balance " +
                        whereClause+" group by account_id order by account_id asc";
        final PreparedStatement statement = this.pinotConnection.prepareStatement(new Request("sql",queryString));
        QueryParamUtil.applyWhereClausesToQuery(whereClauses, statement);
        final ResultSetGroup pinotResultSetGroup = statement.execute();
        final ResultSet resultTableSet = pinotResultSetGroup.getResultSet(0);
        if(resultTableSet.getRowCount()==0) {
            return JSON.createObjectBuilder();
        }
        final JsonObjectBuilder returnObject =  JSON.createObjectBuilder()
                                                .add("timestamp",resultTableSet.getDouble(0,1))
                                                .add("balance",resultTableSet.getLong(0,0))
                                                .add("tokens",JSON.createArrayBuilder().build());
        return returnObject;
    }

    private JsonObjectBuilder getTransactionsJsonFields(Optional<String> transactionTypeParam, String idParam) {
        final List<QueryParamUtil.WhereClause> whereClauses = new ArrayList<>();
        transactionTypeParam.ifPresent(s -> whereClauses.add(QueryParamUtil.parseQueryString(QueryParamUtil.Type._string,"type",s)));
        final ResultSet resultTableSet = getResultSet(idParam, whereClauses, "ids","transaction", QueryParamUtil.Type._long);
        if(resultTableSet.getRowCount()==0) {
            return JSON.createObjectBuilder();
        }
        final JsonArrayBuilder transactionsArray = JSON.createArrayBuilder();
        for (int i = 0; i < resultTableSet.getRowCount(); i++) {
            final JsonObject fields = parseFromColumn(resultTableSet.getString(i, 8));
            final JsonArray transfers = parseArrayFromColumn(resultTableSet.getString(i, 15));
            transactionsArray.add(TransactionsService.getTransactionsJsonObjectBuilder(resultTableSet, i, fields, transfers).build());
        }
        final JsonObjectBuilder returnObject = JSON.createObjectBuilder()
                .add("transactions", transactionsArray.build());
        return returnObject;
    }

    private JsonObjectBuilder getAccountsJsonFields(String idParam) {
        final List<QueryParamUtil.WhereClause> whereClauses = new ArrayList<>();
        String columnName = "";
        QueryParamUtil.Type type = QueryParamUtil.Type._string;
        if(!idParam.isBlank()){
            if(idParam.matches("0.0.\\d+|\\d+")) {
                columnName = "entity_number";
                type = QueryParamUtil.Type._long;
            }
            else if(idParam.matches("0x[abcdef0-9]+"))
                columnName = "evm_address";
            else if(idParam.matches("[A-Za-z0-9]+"))
                columnName = "alias";
        } else {
            return getErrorMessage();
        }
        final ResultSet resultTableSet = getResultSet(idParam, whereClauses, columnName, "entity", type);
        if(resultTableSet.getRowCount()==0) {
            return null;
        }
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

    private ResultSet getResultSet(String param, List<QueryParamUtil.WhereClause> whereClauses, String column_name, String tableName, QueryParamUtil.Type paramType) {
        whereClauses.add(QueryParamUtil.parseQueryString(paramType,column_name,param));
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
