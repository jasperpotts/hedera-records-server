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
import com.swirlds.recordserver.util.QueryParamUtil.WhereClause;
import com.swirlds.recordserver.util.QueryParamUtil.Comparator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.logging.Logger;
import com.swirlds.recordserver.model.BalanceResponse;

import static com.swirlds.recordserver.util.QueryParamUtil.parseLimitQueryString;
import static com.swirlds.recordserver.util.ServiceUtil.getAccountIdRangeFilter;
import static com.swirlds.recordserver.util.ServiceUtil.getAccountBalanceRangeFilter;
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
        rules.get("/", this::getAccountsListMessageHandler);
        rules.get("/{idOrAliasOrEvmAddress}/nfts", this::getAccountNftsMessageHandler);
        rules.get("/{idOrAliasOrEvmAddress}", this::getAccountsMessageHandler);
    }

    private void getAccountsListMessageHandler(ServerRequest request, ServerResponse response) {
        final Optional<String> accountBalanceParam = request.queryParams().first("account.balance");
        final Optional<String> accountIdParam = request.queryParams().first("account.id");
        final Optional<String> publicKeyParam = request.queryParams().first("account.publickey");
        final Optional<String> balanceParam = request.queryParams().first("balance");
        final Optional<String> limitParam = request.queryParams().first("limit");
        final Optional<String> orderParam = request.queryParams().first("order");
        boolean includeBalance = false;
        if(balanceParam.isPresent()) {
            includeBalance = QueryParamUtil.parseQueryString(QueryParamUtil.Type._string, "token_id", balanceParam.get()).equals("true");
        }
        final int limit = parseLimitQueryString(limitParam);

        JsonObjectBuilder accountResponseBuilder;
        java.util.HashMap<Long, BalanceResponse> balanceMap = new java.util.HashMap<>();
        //TODO: BalanceParam
        if(!accountIdParam.isPresent()) {
            final List<QueryParamUtil.WhereClause> whereClauses = new ArrayList<>();

            //Add this for balances table
            final QueryParamUtil.WhereClause accountBalanceWhereClause = accountBalanceParam.isPresent() ?
                    QueryParamUtil.parseQueryString(QueryParamUtil.Type._long, "balance", accountBalanceParam.get()) : null;
            java.util.Set<Long> ids = new java.util.HashSet<>();
            String inEntityNumber = "";
            if(accountBalanceWhereClause != null) {
                getAccountBalanceRangeFilter(accountBalanceParam, whereClauses, limit, accountBalanceWhereClause);
                whereClauses.add(new WhereClause(QueryParamUtil.Type._long, "token_id", Comparator.eq, "0"));
                balanceMap = getBalanceAccountIds(whereClauses);
                ids.addAll(balanceMap.keySet());
                inEntityNumber = " and entity_number IN "+ ids ;
            }

            whereClauses.clear();
            publicKeyParam.ifPresent(s -> whereClauses.add(QueryParamUtil.parseQueryString(QueryParamUtil.Type._string, "public_key", s)));
            final String whereClause = whereClauses.isEmpty() && inEntityNumber.isEmpty() ? "" : "where " + QueryParamUtil.whereClausesToQuery(whereClauses);
            //Get account information for these ids
            final String queryString = "select  alias, max(consensus_timestamp) as consensus_timestamp, entity_number,evm_address,fields,LASTWITHTIME(public_key,consensus_timestamp,'String')as public_key, public_key_type, type from entity" +
                                          whereClause + inEntityNumber +
                                        " group by entity_number,evm_address,alias,type,fields,public_key_type order by entity_number asc limit "+limit;
            final PreparedStatement statement = this.pinotConnection.prepareStatement(new Request("sql", queryString));
            QueryParamUtil.applyWhereClausesToQuery(whereClauses, statement);
            final ResultSetGroup pinotResultSetGroup = statement.execute();
            final ResultSet resultTableSet = pinotResultSetGroup.getResultSet(0);
             accountResponseBuilder = getAccountResponseJsonObjectBuilder(resultTableSet, includeBalance, balanceMap);

        } else {

            final List<QueryParamUtil.WhereClause> whereClauses = new ArrayList<>();
            accountBalanceParam.ifPresent(s -> whereClauses.add(QueryParamUtil.parseQueryString(QueryParamUtil.Type._long, "balance", s)));
            accountIdParam.ifPresent(s -> whereClauses.add(QueryParamUtil.parseQueryString(QueryParamUtil.Type._string, "account_id", s)));
            balanceMap = getBalanceAccountIds(whereClauses);
            whereClauses.clear();
            publicKeyParam.ifPresent(s -> whereClauses.add(QueryParamUtil.parseQueryString(QueryParamUtil.Type._string, "public_key", s)));
            String orderBy = getOrderBy(orderParam);
            final QueryParamUtil.WhereClause accountWhereClause = accountIdParam.isPresent() ?
                    QueryParamUtil.parseQueryString(QueryParamUtil.Type._long, "entity_number", accountIdParam.get()) : null;
            boolean singleAccountMode = getAccountIdRangeFilter(accountIdParam, whereClauses, limit, accountWhereClause);

            final String whereClause = whereClauses.isEmpty() ? "" : "where " + QueryParamUtil.whereClausesToQuery(whereClauses);
            final String queryString =
                    "select * from entity " +
                            whereClause + " order by entity_number " + orderBy;
            System.out.println("queryString = " + queryString);
            final PreparedStatement statement = this.pinotConnection.prepareStatement(new Request("sql", queryString));
            QueryParamUtil.applyWhereClausesToQuery(whereClauses, statement);
            final ResultSetGroup pinotResultSetGroup = statement.execute();
            final ResultSet resultTableSet = pinotResultSetGroup.getResultSet(0);
             accountResponseBuilder = getAccountResponseJsonObjectBuilder(resultTableSet, includeBalance, balanceMap);
        }
        response.send(accountResponseBuilder.build());

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
        return getBalanceResponseObjectBuilder(whereClauses);
    }

    //TODO: Change this query to group by account_id and token_id
    private JsonObjectBuilder getBalanceResponseObjectBuilder(List<QueryParamUtil.WhereClause> whereClauses) {
        final String whereClause = whereClauses.isEmpty() ? "" : "where "+ com.swirlds.recordserver.util.QueryParamUtil.whereClausesToQuery(whereClauses);
        final String queryString =
                "select account_id, token_id, LASTWITHTIME(balance,consensus_timestamp,'long') as balance, max(consensus_timestamp) as consensus_timestamp from balance " +
                        whereClause+" group by account_id,token_id order by account_id asc";
        final org.apache.pinot.client.PreparedStatement statement = this.pinotConnection.prepareStatement(new org.apache.pinot.client.Request("sql",queryString));
        com.swirlds.recordserver.util.QueryParamUtil.applyWhereClausesToQuery(whereClauses, statement);
        final org.apache.pinot.client.ResultSetGroup pinotResultSetGroup = statement.execute();
        final org.apache.pinot.client.ResultSet resultTableSet = pinotResultSetGroup.getResultSet(0);
        if(resultTableSet.getRowCount()==0) {
            return JSON.createObjectBuilder();
        }
        final JsonObjectBuilder returnObject =  JSON.createObjectBuilder()
                                                .add("timestamp",resultTableSet.getDouble(0,3))
                                                .add("balance",resultTableSet.getLong(0,2))
                                                .add("tokens",JSON.createArrayBuilder().build());
        return returnObject;
    }

    private java.util.HashMap<Long,BalanceResponse> getBalanceAccountIds(List<QueryParamUtil.WhereClause> whereClauses) {
        final String whereClause = whereClauses.isEmpty() ? "" : "where "+ com.swirlds.recordserver.util.QueryParamUtil.whereClausesToQuery(whereClauses);
        final String queryString =
                "select account_id, token_id, LASTWITHTIME(balance,consensus_timestamp,'long') as balance, max(consensus_timestamp) as consensus_timestamp from balance " +
                        whereClause+" group by account_id,token_id order by account_id asc";
        final org.apache.pinot.client.PreparedStatement statement = this.pinotConnection.prepareStatement(new org.apache.pinot.client.Request("sql",queryString));
        com.swirlds.recordserver.util.QueryParamUtil.applyWhereClausesToQuery(whereClauses, statement);
        final org.apache.pinot.client.ResultSetGroup pinotResultSetGroup = statement.execute();
        final org.apache.pinot.client.ResultSet resultTableSet = pinotResultSetGroup.getResultSet(0);
        if(resultTableSet.getRowCount()==0) {
            return new java.util.HashMap<Long,BalanceResponse>();
        }
        java.util.HashMap<Long,BalanceResponse> balanceMap = new java.util.HashMap<>();
        for(int i=0; i<resultTableSet.getRowCount(); i++) {
            long id = resultTableSet.getLong(0,1);
            balanceMap.put(id,new BalanceResponse(id, resultTableSet.getDouble(0,3), resultTableSet.getLong(0,2), JSON.createArrayBuilder().build()));
        }
        return balanceMap;
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
        return getAccountResponseJsonObjectBuilder(resultTableSet, false, new java.util.HashMap<>());
    }

    private JsonObjectBuilder getAccountResponseJsonObjectBuilder(org.apache.pinot.client.ResultSet resultTableSet, boolean includeBalance, java.util.HashMap<Long, com.swirlds.recordserver.model.BalanceResponse> balanceMap) {
        final JsonArrayBuilder accountsArray = JSON.createArrayBuilder();

        if(resultTableSet.getRowCount() == 0) {
            return JSON.createObjectBuilder();
        }
        for (int i = 0; i < resultTableSet.getRowCount(); i++) {
            final JsonObject fields = parseFromColumn(resultTableSet.getString(i, 4));
            long accountId = resultTableSet.getLong(i, 2);
            final JsonObjectBuilder accountsObject = JSON.createObjectBuilder()
                    .add("account", accountId)
                    .add("alias", resultTableSet.getString(i, 0))
                    .add("auto_renew_period", fields.getString("auto_renew_period", ""))
                    .add("decline_reward", fields.getString("decline_reward", ""))
                    .add("deleted", fields.getString("deleted", ""))
                    .add("ethereum_nonce", fields.getString("ethereum_nonce", ""))
                    .add("expiry_timestamp", fields.getString("expiry_timestamp", ""))
                    .add("key", JSON.createObjectBuilder()
                            .add("_type", resultTableSet.getString(i, 6))
                            .add("key", resultTableSet.getString(i, 5)))
                    .add("max_automatic_token_associations", fields.getString("max_automatic_token_associations", ""))
                    .add("memo", fields.getString("memo", ""))
                    .add("receiver_sig_required", fields.getString("receiver_sig_required", ""))
                    .add("staked_account_id", fields.getString("staked_account_id", ""))
                    .add("staked_node_id", fields.getString("staked_node_id", ""))
                    .add("stake_period_start", fields.getString("stake_period_start", ""))
                    .add("evm_address", resultTableSet.getString(i, 3));
            if(includeBalance) {
                accountsObject.add("balance",balanceMap.get(accountId).toJsonObject());
            }
            accountsArray.add(accountsObject);
        }
        // if i =1 send only account object
        final JsonObjectBuilder returnObject = JSON.createObjectBuilder()
                .add("accounts", accountsArray.build());
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
