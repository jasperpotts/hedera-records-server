package com.swirlds.recordserver.util;

import org.apache.pinot.client.PreparedStatement;

import java.util.List;
import java.util.Optional;

public class QueryParamUtil {
	public enum Comparator {
		eq("="),
		ne("<>"),
		lt("<"),
		lte("<="),
		gt(">"),
		gte(">=");
		private final String sql;

		Comparator(final String sql) {
			this.sql = sql;
		}

		public static Comparator parse(String str) {
			if (str != null) {
				try {
					return Comparator.valueOf(str.toLowerCase());
				} catch (IllegalArgumentException e) {
					// ignore
				}
			}
			return Comparator.eq;
		}
	}
	public enum Order {
		asc, desc;

		public static Order parse(Optional<String> str) {
			if (str.isPresent()) {
				try {
					return Order.valueOf(str.get().toLowerCase());
				} catch (IllegalArgumentException e) {
					// ignore
				}
			}
			return Order.asc;
		}
	}
	public enum Type {_string, _int, _long, _float, _double }
	public record WhereClause(Type columnType, String columnName, Comparator comparator, String value){
		@Override
		public String toString() {
			return "WhereClause{" +
					"columnType=" + columnType +
					", columnName='" + columnName + '\'' +
					", comparator=" + comparator +
					", value='" + value + '\'' +
					'}';
		}
	};

	public static WhereClause parseQueryString(Type columnType, String columnName, String queryString) {
		String[] parts = queryString.split(":");
		Comparator comparator = parts.length > 1 ? Comparator.parse(parts[0]) : Comparator.eq;
		String value = parts.length > 1 ? parts[1] : parts[0];
		if (value.matches("\\d+\\.\\d+\\.\\d+")) {
			value = value.split("\\.")[2];
		}
		return new WhereClause(columnType, columnName, comparator, value);
	}

	public static int parseLimitQueryString(Optional<String> limit) {
		if (limit.isPresent()) {
			try {
				return Integer.parseInt(limit.get().trim());
			} catch (NumberFormatException e) {
				e.printStackTrace();
			}
		}
		return 25;
	}

	public static String whereClausesToQuery(List<WhereClause> whereClauses) {
		final StringBuilder stringBuilder = new StringBuilder();
		for (int i = 0; i < whereClauses.size(); i++) {
			WhereClause clause = whereClauses.get(i);
			stringBuilder.append(clause.columnName);
			stringBuilder.append(' ');
			stringBuilder.append(clause.comparator.sql);
			stringBuilder.append(" ? ");
			if (i<(whereClauses.size()-1)) stringBuilder.append(" and ");
		}
		return  stringBuilder.toString();
	}

	public static void applyWhereClausesToQuery(List<WhereClause> whereClauses, PreparedStatement statement) {
		for (int i = 0; i < whereClauses.size(); i++) {
			final WhereClause clause = whereClauses.get(i);
			switch(clause.columnType) {
				case _string -> statement.setString(i, clause.value);
				case _int -> statement.setInt(i, Integer.parseInt(clause.value));
				case _long -> statement.setLong(i, Long.parseLong(clause.value));
				case _float -> statement.setFloat(i, Float.parseFloat(clause.value));
				case _double -> statement.setDouble(i, Double.parseDouble(clause.value));
			}
		}
	}
}
