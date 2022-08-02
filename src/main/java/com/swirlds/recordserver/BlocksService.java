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
 * A service for blocks API
 */
public class BlocksService implements Service {

    private static final Logger LOGGER = Logger.getLogger(BlocksService.class.getName());
    private static final JsonBuilderFactory JSON = Json.createBuilderFactory(Collections.emptyMap());
    private final MetricRegistry registry = RegistryFactory.getInstance().getRegistry(MetricRegistry.Type.APPLICATION);
    private final Counter accessCtr = registry.counter("accessctr");
    private final Connection pinotConnection;

    BlocksService(Config config) {
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
        final Optional<String> blockNumberQueryParam = request.queryParams().first("block.number");
        final Optional<String> timestampsParam = request.queryParams().first("timestamp");
        final Optional<String> limitParam = request.queryParams().first("limit");
        final Optional<String> orderParam = request.queryParams().first("order");

        // build and execute query
        final List<QueryParamUtil.WhereClause> whereClauses = new ArrayList<>();
        blockNumberQueryParam.ifPresent(s -> whereClauses.add(QueryParamUtil.parseQueryString(QueryParamUtil.Type._long,"number",s)));
        timestampsParam.ifPresent(s -> whereClauses.add(QueryParamUtil.parseQueryString(QueryParamUtil.Type._long,"consensus_timestamp",s)));
        final String whereClause = whereClauses.isEmpty() ? "" : "where "+QueryParamUtil.whereClausesToQuery(whereClauses);
        final String queryString =
                "select address_books, consensus_end_timestamp, consensus_start_timestamp, data_hash, fields_1, number, prev_hash, signature_files_1 from record_new " +
                        whereClause+" order by number "+QueryParamUtil.Order.parse(orderParam).toString()+" limit ?";
        final PreparedStatement statement = this.pinotConnection.prepareStatement(new Request("sql",queryString));
        QueryParamUtil.applyWhereClausesToQuery(whereClauses, statement);
        statement.setInt(whereClauses.size(), parseLimitQueryString(limitParam)); // limit
        final ResultSetGroup pinotResultSetGroup = statement.execute();
        final ResultSet resultTableResultSet = pinotResultSetGroup.getResultSet(0);

        // format results to JSON
        long highestAccountNumber = 0;
        final JsonArrayBuilder balancesArray = JSON.createArrayBuilder();
        for (int i = 0; i < resultTableResultSet.getRowCount(); i++) {
            final long blockNum = resultTableResultSet.getLong(i,5);
            highestAccountNumber = Math.max(highestAccountNumber,blockNum);
            final JsonObject fields = parseFromColumn(resultTableResultSet.getString(i,4));
            balancesArray.add(JSON.createObjectBuilder()
                    .add("count",1)
                    .add("hapi_version",fields.getString("hapi_version"))
                    .add("hash","0x"+ resultTableResultSet.getString(i, 3))
                    .add("name",fields.getString("name"))
                    .add("number",blockNum)
                    .add("prev_hash","0x"+resultTableResultSet.getString(i,6))
                    .add("size",fields.getString("size"))
                    .add("timestamp",JSON.createObjectBuilder()
                            .add("from",resultTableResultSet.getLong(i,2))
                            .add("to",resultTableResultSet.getLong(i,1))
                            .build())
                    .add("gas_used",fields.getString("gas_used"))
                    .add("logs_bloom",fields.getString("logs_bloom"))
                    .build());
        }
        final JsonObjectBuilder returnObject = JSON.createObjectBuilder()
                .add("blocks", balancesArray.build())
                .add("links", JSON.createObjectBuilder()
                        .add("next","/api/v1/blocks?order=asc&limit=10&number=gt:0.0."+highestAccountNumber)
                        .build())
                ;
        response.send(returnObject.build());
    }
}