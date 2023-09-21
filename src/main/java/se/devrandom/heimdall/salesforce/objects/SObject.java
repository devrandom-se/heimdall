/*
 * Heimdall - Salesforce Backup Solution
 * Copyright (C) 2025 Johan Karlsteen
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package se.devrandom.heimdall.salesforce.objects;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.devrandom.heimdall.batchprocessing.SObjectDescriptionProcessor;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.ParameterizedType;
import java.text.SimpleDateFormat;
import java.util.*;

public abstract class SObject {
    private static final Logger log = LoggerFactory.getLogger(SObject.class);

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface SalesforceField {
        public boolean readOnly() default false;
        public boolean externalId() default false;
        public boolean comparable() default true;
        public boolean insertOnly() default false;
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface SalesforceChildRelation {}
    @SalesforceField(readOnly = true) public String Id;
    @SalesforceField public String Name;
    @SalesforceField(readOnly = true) public String CreatedById;
    @SalesforceField(readOnly = true) public String LastModifiedById;

    public SObject() {}
    public SObject(String name) {
        this.setName(name);
    }

    public String getId() {
        return Id;
    }

    public void setId(String id) {
        Id = id;
    }

    public String getName() {
        return Name;
    }

    public void setName(String name) {
        Name = name;
    }

    public String getCreatedById() {
        return CreatedById;
    }

    public void setCreatedById(String createdById) {
        CreatedById = createdById;
    }

    public String getLastModifiedById() {
        return LastModifiedById;
    }

    public void setLastModifiedById(String lastModifiedById) {
        LastModifiedById = lastModifiedById;
    }

    public String buildSoqlQuery(WhereOrderClause whereOrderClause, Map<String,WhereOrderClause> subQueryClauses, String subQueryName) {
        List<String> fieldNames = new ArrayList<>();
        List<String> subQueries = new ArrayList<>();
        Class<?> i = this.getClass();
        while (i != null && i != Object.class) {
            for(Field f : i.getDeclaredFields()) {
                if(f.isAnnotationPresent(SalesforceField.class)) {
                    fieldNames.add(f.getName());
                } else if(f.isAnnotationPresent(SalesforceChildRelation.class)) {
                    try {
                        ParameterizedType t = (ParameterizedType) f.getGenericType();
                        Class cls = (Class)t.getActualTypeArguments()[0];
                        SObject subQueryObject = (SObject) cls.getDeclaredConstructor().newInstance();
                        Optional<WhereOrderClause> subQueryWhereOrderClause = Optional.ofNullable(subQueryClauses.get(f.getName()));
                        subQueries.add(subQueryObject.buildSoqlQuery(subQueryWhereOrderClause.orElseThrow(), null, f.getName()));
                    } catch (InstantiationException | IllegalAccessException | InvocationTargetException |
                             NoSuchMethodException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
            i = i.getSuperclass();
        }
        String sobjectName = this.getClass().getSimpleName();
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("SELECT %s",
                String.join(",",fieldNames), sobjectName));
        for(String subQuery : subQueries) {
            sb.append(String.format(",(%s)", subQuery));
        }
        sb.append(String.format(" FROM %s", Optional.ofNullable(subQueryName).orElse(sobjectName)));
        Optional<WhereOrderClause> optionalWhereOrderClause = Optional.ofNullable(whereOrderClause);
        if(optionalWhereOrderClause.isPresent()) {
            if(optionalWhereOrderClause.get().getWhereClause().isPresent()) {
                sb.append(String.format(" WHERE %s", optionalWhereOrderClause.get().getWhereClause().get()));
            }
            if(optionalWhereOrderClause.get().getOrderClause().isPresent()) {
                sb.append(String.format(" ORDER BY %s", optionalWhereOrderClause.get().getOrderClause().get()));
            }
            if(optionalWhereOrderClause.get().getLimit().isPresent()) {
                sb.append(String.format(" LIMIT %d", optionalWhereOrderClause.get().getLimit().get()));
            }
        }
        return sb.toString().replace(" ", " ");
    }

    String createDateString(Date date) {
        SimpleDateFormat formatter = new SimpleDateFormat(
                "yyyy-MM-dd");
        return formatter.format(date);
    }

    String createDateTimeString(Date date) {
        // Format: 2023-09-21T08:46:46.000Z (ISO 8601 with milliseconds and Z for UTC)
        SimpleDateFormat formatter = new SimpleDateFormat(
                "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        formatter.setTimeZone(java.util.TimeZone.getTimeZone("UTC"));
        return formatter.format(date);
    }

    public String generateJsonBody(Boolean isInsert) {
        JSONObject jo = new JSONObject();
        Class<?> i = this.getClass();
        while (i != null && i != Object.class) {
            for (Field f : i.getDeclaredFields()) {
                if (f.isAnnotationPresent(SalesforceField.class)) {
                    if(!f.getAnnotation(SalesforceField.class).readOnly()
                            && !f.getAnnotation(SalesforceField.class).externalId()
                            && !(!isInsert && f.getAnnotation(SalesforceField.class).insertOnly())) {
                        Optional val = Optional.empty();
                        f.setAccessible(true);
                        try {
                            if(f.getType().equals(Date.class)) {
                                Date d = (Date)f.get(this);
                                if(d != null) {
                                    // Use datetime format with milliseconds for all Date fields
                                    // Salesforce accepts this for both Date and DateTime fields
                                    val = Optional.ofNullable(createDateTimeString(d));
                                }
                            } else {
                                val = Optional.ofNullable(f.get(this));
                            }
                            if(val.isPresent()) {
                                jo.put(f.getName(), val.get());
                            }
                        } catch (IllegalAccessException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
            }
            i = i.getSuperclass();
        }
        return jo.toString();
    }

    public static class WhereOrderClause {
        private final String whereClause;
        private final String orderClause;
        private final Integer limit;

        public WhereOrderClause(String whereClause, String orderClause, Integer limit) {
            this.whereClause = Optional.of(whereClause).orElse(null);
            this.orderClause = Optional.of(orderClause).orElse(null);
            this.limit = Optional.of(limit).orElse(null);
        }

        public Optional<String> getWhereClause() {
            return Optional.ofNullable(whereClause);
        }

        public Optional<String> getOrderClause() {
            return Optional.ofNullable(orderClause);
        }

        public Optional<Integer> getLimit() {
            return Optional.ofNullable(limit);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        if (obj.getClass() != this.getClass()) {
            return false;
        }

        final Heimdall_Backup_Config__c other = (Heimdall_Backup_Config__c) obj;
        Class<?> i = this.getClass();
        while (i != null && i != Object.class) {
            for (Field f : i.getDeclaredFields()) {
                if (f.isAnnotationPresent(SalesforceField.class)
                        && f.getAnnotation(SalesforceField.class).comparable()) {
                    f.setAccessible(true);
                    log.trace("Comparing %s", f.getName());
                    try {
                        Object thisValue = f.get(this);
                        Object otherValue = f.get(other);
                        if (thisValue == null ? otherValue != null : !thisValue.equals(otherValue)) {
                            return false;
                        }
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
            i = i.getSuperclass();
        }

        return true;
    }
}
