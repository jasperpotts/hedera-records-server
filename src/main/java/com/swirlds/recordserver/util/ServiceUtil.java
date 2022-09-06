package com.swirlds.recordserver.util;

public class ServiceUtil {

    public static boolean getAccountIdRangeFilter(java.util.Optional<String> accountIdParam, java.util.List<com.swirlds.recordserver.util.QueryParamUtil.WhereClause> whereClauses, int limit, com.swirlds.recordserver.util.QueryParamUtil.WhereClause accountWhereClause) {
        long minAccount, maxAccount;
        boolean singleAccountMode = false;
        if (accountIdParam.isPresent()) {
            if (accountWhereClause.comparator() == com.swirlds.recordserver.util.QueryParamUtil.Comparator.eq) {
                whereClauses.add(accountWhereClause);
                singleAccountMode = true;
            } else if (accountWhereClause.comparator() == com.swirlds.recordserver.util.QueryParamUtil.Comparator.ne){
                // TODO, not quite sure what this should do
                singleAccountMode = true;
            } else {
                switch (accountWhereClause.comparator()) {
                    case lt -> {
                        maxAccount = Long.parseLong(accountWhereClause.value());
                        minAccount = maxAccount- limit;
                    }
                    case lte -> {
                        maxAccount = Long.parseLong(accountWhereClause.value())+1;
                        minAccount = maxAccount- limit;
                    }
                    case gt -> {
                        minAccount = Long.parseLong(accountWhereClause.value());
                        maxAccount = minAccount+ limit;
                    }
                    case gte -> {
                        minAccount = Long.parseLong(accountWhereClause.value())-1;
                        maxAccount = minAccount+ limit;
                    }
                    default -> {
                        minAccount = 0;
                        maxAccount = minAccount+ limit;
                    }
                }
                addRangeToWhereClauses(whereClauses, "account_id", minAccount, maxAccount);
            }
        } else {
            minAccount = 0;
            maxAccount = limit;
            addRangeToWhereClauses(whereClauses, "account_id", minAccount, maxAccount);
        }
        return singleAccountMode;
    }
    public static void getAccountBalanceRangeFilter(java.util.Optional<String> accountBalanceParam, java.util.List<com.swirlds.recordserver.util.QueryParamUtil.WhereClause> whereClauses, int limit, com.swirlds.recordserver.util.QueryParamUtil.WhereClause accountBalanceWhereClause) {
        long minBalance, maxBalance;
        if (accountBalanceParam.isPresent()) {
            if (accountBalanceWhereClause.comparator() == com.swirlds.recordserver.util.QueryParamUtil.Comparator.eq) {
                whereClauses.add(accountBalanceWhereClause);
            } else if (accountBalanceWhereClause.comparator() == com.swirlds.recordserver.util.QueryParamUtil.Comparator.ne){
                // TODO, not quite sure what this should do
            } else {
                switch (accountBalanceWhereClause.comparator()) {
                    case lt -> {
                        maxBalance = Long.parseLong(accountBalanceWhereClause.value());
                        minBalance = maxBalance- limit;
                    }
                    case lte -> {
                        maxBalance = Long.parseLong(accountBalanceWhereClause.value())+1;
                        minBalance = maxBalance- limit;
                    }
                    case gt -> {
                        minBalance = Long.parseLong(accountBalanceWhereClause.value());
                        maxBalance = minBalance+ limit;
                    }
                    case gte -> {
                        minBalance = Long.parseLong(accountBalanceWhereClause.value())-1;
                        maxBalance = minBalance+ limit;
                    }
                    default -> {
                        minBalance = 0;
                        maxBalance = minBalance+ limit;
                    }
                }
                addRangeToWhereClauses(whereClauses, "balance", minBalance, maxBalance);
            }
        } else {
            minBalance = 0;
            maxBalance = limit;
            addRangeToWhereClauses(whereClauses, "balance", minBalance, maxBalance);
        }
    }

    private static void addRangeToWhereClauses(java.util.List<com.swirlds.recordserver.util.QueryParamUtil.WhereClause> whereClauses, String column, long minBalance, long maxBalance) {
        whereClauses.add(new com.swirlds.recordserver.util.QueryParamUtil.WhereClause(com.swirlds.recordserver.util.QueryParamUtil.Type._long, column,
                com.swirlds.recordserver.util.QueryParamUtil.Comparator.gt, Long.toString(minBalance)));
        whereClauses.add(new com.swirlds.recordserver.util.QueryParamUtil.WhereClause(com.swirlds.recordserver.util.QueryParamUtil.Type._long, column,
                com.swirlds.recordserver.util.QueryParamUtil.Comparator.lt, Long.toString(maxBalance)));
    }

}
