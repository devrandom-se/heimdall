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

import java.util.List;

public class Field {
    public String name;
    public String type;
    public Boolean createable;
    public Boolean updateable;
    public String label;
    public Boolean nillable;
    public Object defaultValue;
    public List<String> referenceTo;
    public List<PicklistValue> picklistValues;
    public Integer length;

    public static class PicklistValue {
        public Boolean active;
        public String value;
        public String label;
    }
}
