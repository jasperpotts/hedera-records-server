package com.swirlds.recordserver.model;

public class BalanceResponse {

    private static final jakarta.json.JsonBuilderFactory JSON = jakarta.json.Json.createBuilderFactory(java.util.Collections.emptyMap());

    Long id;

    Double timestamp;

    Long balance;

    jakarta.json.JsonArray tokens;

    public Long getId() {
        return id;
    }

    public Double getTimestamp() {
        return timestamp;
    }

    public Long getBalance() {
        return balance;
    }

    public jakarta.json.JsonArray getTokens() {
        return tokens;
    }

    public BalanceResponse(Long id, Double timestamp, Long balance, jakarta.json.JsonArray tokens) {
        this.id = id;
        this.timestamp = timestamp;
        this.balance = balance;
        this.tokens = tokens;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public void setTimestamp(Double timestamp) {
        this.timestamp = timestamp;
    }

    public void setBalance(Long balance) {
        this.balance = balance;
    }

    public void setTokens(jakarta.json.JsonArray tokens) {
        this.tokens = tokens;
    }

    public jakarta.json.JsonObject toJsonObject() {
            jakarta.json.JsonObjectBuilder builder = JSON.createObjectBuilder().add("balance",this.getBalance())
                                                                            .add("timestamp",this.getTimestamp())
                                                                            .add("tokens",JSON.createArrayBuilder().build());
            return builder.build();
        }
}
