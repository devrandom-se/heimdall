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

import java.text.SimpleDateFormat;
import java.util.Date;

public class TrackChanges<T> {
    private T value;
    private boolean isDirty;
    public TrackChanges(T value) {
        this.value = value;
    }

    public T get() {
        return value;
    }

    public void set(T field) {
        if(!this.value.equals(field)) {
            this.value = field;
            this.isDirty = true;
        }
    }

    public boolean isDirty() {
        return isDirty;
    }

    private String createDateString(Date date) {
        SimpleDateFormat formatter = new SimpleDateFormat(
                "yyyy-MM-dd");
        return formatter.format(date);
    }
    public String stringValue() {
        if(value == null) {
            return "";
        } else if(value instanceof Date) {
            return createDateString((Date) value);
        }  else {
            return value.toString();
        }
    }
}
