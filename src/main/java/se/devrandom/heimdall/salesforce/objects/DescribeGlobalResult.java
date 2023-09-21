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

import java.util.Map;

public class DescribeGlobalResult {
    public Boolean activateable;
    public Boolean custom;
    public Boolean customSetting;
    public Boolean createable;
    public Boolean deletable;
    public Boolean deprecatedAndHidden;
    public Boolean feedEnabled;
    public String keyPrefix;
    public String label;
    public String labelPlural;
    public Boolean layoutable;
    public Boolean mergeable;
    public Boolean mruEnabled;
    public String name;
    public Boolean queryable;
    public Boolean replicateable;
    public Boolean retrieveable;
    public Boolean searchable;
    public Boolean triggerable;
    public Boolean undeletable;
    public Boolean updateable;
    public Map<String,String> urls;
}
